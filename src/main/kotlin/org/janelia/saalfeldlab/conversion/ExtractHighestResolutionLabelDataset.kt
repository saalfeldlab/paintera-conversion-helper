package org.janelia.saalfeldlab.conversion

import com.pivovarit.function.ThrowingConsumer
import gnu.trove.map.TLongLongMap
import gnu.trove.map.hash.TLongLongHashMap
import io.github.oshai.kotlinlogging.KotlinLogging
import net.imglib2.FinalInterval
import net.imglib2.Interval
import net.imglib2.RandomAccessibleInterval
import net.imglib2.algorithm.util.Grids
import net.imglib2.img.array.ArrayImgs
import net.imglib2.loops.LoopBuilder
import net.imglib2.algorithm.util.Singleton
import net.imglib2.algorithm.util.Singleton.ThrowingSupplier
import net.imglib2.converter.Converter
import net.imglib2.converter.Converters
import net.imglib2.type.NativeType
import net.imglib2.type.label.LabelMultisetType
import net.imglib2.type.numeric.IntegerType
import net.imglib2.type.numeric.integer.UnsignedLongType
import net.imglib2.util.Intervals
import net.imglib2.util.Pair
import net.imglib2.view.Views
import org.apache.http.client.utils.URIBuilder
import org.apache.http.message.BasicNameValuePair
import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.n5.DataType
import org.janelia.saalfeldlab.n5.LongArrayDataBlock
import org.janelia.saalfeldlab.n5.N5Reader
import org.janelia.saalfeldlab.n5.imglib2.N5LabelMultisets
import org.janelia.saalfeldlab.n5.imglib2.N5Utils
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3KeyValueWriter
import org.janelia.saalfeldlab.n5.spark.supplier.N5ReaderSupplier
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier
import org.janelia.scicomp.n5.zstandard.ZstandardCompression
import picocli.CommandLine
import scala.Tuple2
import java.io.IOException
import java.io.Serializable
import java.nio.ByteBuffer
import java.util.Optional
import java.util.function.Supplier
import java.util.stream.Collectors

object ExtractHighestResolutionLabelDataset {
	private val LOG = KotlinLogging.logger { }

	private val VALID_TYPES: Set<DataType> = setOf(
		DataType.UINT64, DataType.UINT32, DataType.INT64
	)

	private fun isValidType(dataType: DataType): Boolean {
		return VALID_TYPES.contains(dataType)
	}

	@Throws(IOException::class)
	fun extractNoGenerics(
		sc: JavaSparkContext,
		n5in: N5ReaderSupplier,
		n5out: N5WriterSupplier,
		datasetIn: String?,
		datasetOut: String?,
		blockSizeOut: IntArray?,
		considerFragmentSegmentAssignment: Boolean,
		assignment: TLongLongMap,
		chunksPerShard: IntArray? = null
	) {
		extract(sc, n5in, n5out, datasetIn, datasetOut, blockSizeOut, considerFragmentSegmentAssignment, assignment, chunksPerShard)
	}

	@JvmStatic
	@Throws(IOException::class)
	fun <IN> extract(
		sc: JavaSparkContext,
		n5in: N5ReaderSupplier,
		n5out: N5WriterSupplier,
		datasetIn: String?,
		datasetOut: String?,
		blockSizeOut: IntArray?,
		considerFragmentSegmentAssignment: Boolean,
		assignment: TLongLongMap,
		chunksPerShard: IntArray? = null
	) where IN : NativeType<IN>?, IN : IntegerType<IN>? {
		extract<IN, UnsignedLongType>(
			sc,
			n5in,
			n5out,
			datasetIn,
			datasetOut,
			blockSizeOut,
			object : Supplier<UnsignedLongType>, Serializable {
				override fun get() = UnsignedLongType()
			},
			emptyMap(),
			considerFragmentSegmentAssignment,
			assignment,
			chunksPerShard
		)
	}

	@Throws(IOException::class)
	fun <IN, OUT> extract(
		sc: JavaSparkContext,
		n5in: N5ReaderSupplier,
		n5out: N5WriterSupplier,
		datasetIn: String?,
		datasetOut: String?,
		blockSizeOut: IntArray?,
		outputTypeSupplier: Supplier<OUT>,
		additionalAttributes: Map<String?, Any>,
		considerFragmentSegmentAssignment: Boolean,
		assignment: TLongLongMap,
		chunksPerShard: IntArray? = null
	) where IN : NativeType<IN>?, IN : IntegerType<IN>?, OUT : NativeType<OUT>?, OUT : IntegerType<OUT>? {
		val n5InLocal = n5in.get()
		if (!n5InLocal.exists(datasetIn)) {
			throw IOException(String.format("%s does not exist in container %s", datasetIn, n5InLocal))
		}

		if (!n5InLocal.datasetExists(datasetIn)) {
			if (n5InLocal.listAttributes(datasetIn).containsKey("painteraData")) {
				try {
					val updatedAdditionalEntries: MutableMap<String?, Any> = HashMap(additionalAttributes)
					Optional.ofNullable(n5InLocal.getAttribute(datasetIn, "maxId", Long::class.javaPrimitiveType)).ifPresent { id: Long -> updatedAdditionalEntries["maxId"] = id }
					if (considerFragmentSegmentAssignment) {
						val loadedAssignments = readAssignments(n5InLocal, "$datasetIn/fragment-segment-assignment")
						loadedAssignments.putAll(assignment)
						assignment.clear()
						assignment.putAll(loadedAssignments)
					}
					extract(
						sc, n5in, n5out, "$datasetIn/data", datasetOut, blockSizeOut, outputTypeSupplier, updatedAdditionalEntries,
						considerFragmentSegmentAssignment, assignment, chunksPerShard
					)
					return
				} catch (e: NoValidDatasetException) {
					throw NoValidDatasetException(n5InLocal, datasetIn)
				}
			} else if (n5InLocal.exists("$datasetIn/s0")) {
				try {
					extract(
						sc, n5in, n5out, "$datasetIn/s0", datasetOut, blockSizeOut, outputTypeSupplier, additionalAttributes,
						considerFragmentSegmentAssignment, assignment, chunksPerShard
					)
					return
				} catch (e: NoValidDatasetException) {
					throw NoValidDatasetException(n5InLocal, datasetIn)
				}
			} else throw NoValidDatasetException(n5InLocal, datasetIn)
		}

		val outputIsLabelMultiset = outputTypeSupplier.get() is LabelMultisetType

		val attributesIn = n5InLocal.getDatasetAttributes(datasetIn)
		val dimensions = attributesIn.dimensions.clone()
		val blockSize = blockSizeOut ?: attributesIn.blockSize
		val dataType = DataType.UINT8.takeIf { outputIsLabelMultiset } ?: N5Utils.dataType(outputTypeSupplier.get())

		val outWriter = n5out.get()
		if (chunksPerShard != null && outWriter is ZarrV3KeyValueWriter) {
			/* shard size = chunks-per-shard * block size ( when sharded the block size parameter is used for chunk size; maybe should rename)  */
			val shardSize = IntArray(blockSize.size) { chunksPerShard[it] * blockSize[it] }
			outWriter.createDataset(datasetOut, ZarrV3DatasetAttributes(dimensions, shardSize, blockSize, dataType, ZstandardCompression()))
		} else
			outWriter.createDataset(datasetOut, dimensions, blockSize, dataType, ZstandardCompression())
		/* the unit of parallel write is always the DatasetAttributes#blockSize. when sharded this is the shard size */
		val outputBlockSize = outWriter.getDatasetAttributes(datasetOut).blockSize
		val keys = assignment.keys()
		val values = assignment.values()

		// TODO automate copy of attributes if/when N5 separates attributes from dataset attributes
//        for (Map.Entry<String, Class<?>> entry :n5in.get().listAttributes(datasetIn).entrySet()) {
//            if (DATASET_ATTRIBUTES.contains(entry.getKey()))
//                continue;
//            try {
//                Object attr = n5in.get().getAttribute(datasetIn, entry.getKey(), entry.getValue());
//                LOG.debug("Copying attribute { {}: {} } of type {}", entry.getKey(), attr, entry.getValue());
//                n5out.get().setAttribute(datasetOut, entry.getKey(), attr);
//            } catch (IOException e) {
//                LOG.warn("Unable to copy attribute { {}: {} }", entry.getKey(), entry.getValue());
//                LOG.debug("Unable to copy attribute { {}: {} }", entry.getKey(), entry.getValue(), e);
//            }
//        }
		Optional
			.ofNullable(n5InLocal.getAttribute(datasetIn, "resolution", DoubleArray::class.java))
			.ifPresent(ThrowingConsumer.unchecked { r: DoubleArray -> n5out.get().setAttribute(datasetOut, "resolution", r) })

		Optional
			.ofNullable(n5InLocal.getAttribute(datasetIn, "offset", DoubleArray::class.java))
			.ifPresent(ThrowingConsumer.unchecked { o: DoubleArray -> n5out.get().setAttribute(datasetOut, "offset", o) })

		Optional
			.ofNullable(n5InLocal.getAttribute(datasetIn, "maxId", Long::class.javaPrimitiveType))
			.ifPresent(ThrowingConsumer.unchecked { id: Long -> n5out.get().setAttribute(datasetOut, "maxId", id) })

		additionalAttributes.entries.forEach(ThrowingConsumer.unchecked { e: Map.Entry<String?, Any> -> n5out.get().setAttribute(datasetOut, e.key, e.value) })

		try {
			n5out.get().setAttribute(datasetOut, N5LabelMultisets.LABEL_MULTISETTYPE_KEY, outputIsLabelMultiset)
		} catch (e: IOException) {
			LOG.warn { "Unable to write attribute { ${N5LabelMultisets.LABEL_MULTISETTYPE_KEY}: $outputIsLabelMultiset }" }
			LOG.debug(e) { "Unable to write attribute { ${N5LabelMultisets.LABEL_MULTISETTYPE_KEY}: $outputIsLabelMultiset }" }
		}
		val isLabelMultiset = N5LabelMultisets.isLabelMultisetType(n5InLocal, datasetIn)

		if (!(DataType.UINT8 == attributesIn.dataType && isLabelMultiset || isValidType(attributesIn.dataType) && !isLabelMultiset)) throw InvalidTypeException(attributesIn.dataType, isLabelMultiset)

		val blocks: List<Tuple2<Tuple2<LongArray, LongArray>, LongArray>> = Grids
			.collectAllContainedIntervalsWithGridPositions(dimensions, outputBlockSize)
			.stream()
			.map { p: Pair<Interval, LongArray> -> Tuple2(Tuple2(Intervals.minAsLongArray(p.a), Intervals.maxAsLongArray(p.a)), p.b) }
			.collect(Collectors.toList())

		sc
			.parallelize(blocks)
			.foreach { blockWithPosition: Tuple2<Tuple2<LongArray, LongArray>, LongArray> ->

				val n5Local = n5in.get()
				val imgCacheKey = URIBuilder(n5Local.uri).setParameters(
					BasicNameValuePair("call", "extract-highest-resolution-label-dataset"),
					BasicNameValuePair("dataset", datasetIn)
				).toString()

				val input: RandomAccessibleInterval<IN> = Singleton.get(imgCacheKey, ThrowingSupplier {
					if (isLabelMultiset) N5LabelMultisets.openLabelMultiset(n5Local, datasetIn) as RandomAccessibleInterval<IN>
					else N5Utils.open(n5Local, datasetIn)
				})

				val block: RandomAccessibleInterval<IN> = Views.interval(
					input,
					blockWithPosition._1()._1(),
					blockWithPosition._1()._2()
				)

				val converted = Converters.convert(
					block,
					getAppropriateConverter(TLongLongHashMap(keys, values)),
					outputTypeSupplier.get()
				)

				val n5LocalOut = n5out.get()
				val writerCacheKey = URIBuilder(n5LocalOut.uri).setParameters(
					BasicNameValuePair("call", "extract-highest-resolution-label-dataset"),
					BasicNameValuePair("type", "writer")
				).toString()

				val writer = Singleton.get(writerCacheKey, ThrowingSupplier { n5LocalOut })

				/* read the created dataset's attributes so sharded writes route into shards */
				val attributes = writer.getDatasetAttributes(datasetOut)

				val fillValue = (attributes as? ZarrV3DatasetAttributes)?.run { ByteBuffer.wrap(fillBytes).long } ?: 0L
				var blockIsEmpty = true
				if (attributes.isSharded) {
					/* materialize a full shard-sized block (0-padded past the edge) and write it at the shard grid position.*/
					val shardBlockSize = attributes.blockSize
					val data = LongArray(shardBlockSize.fold(1) { acc, s -> acc * s })
					val shardImg = ArrayImgs.unsignedLongs(data, *shardBlockSize.map { it.toLong() }.toLongArray())
					LoopBuilder.setImages(
						Views.zeroMin(converted),
						Views.interval(shardImg, FinalInterval(*Intervals.dimensionsAsLongArray(converted)))
					).forEachPixel { source, target ->
						val value = source!!.integerLong
						target.setInteger(value)
						/* set blockIsEmpty = false first value that is not the fill value. */
						if (blockIsEmpty && value != fillValue)
							blockIsEmpty = false
					}
					if (!blockIsEmpty)
						writer.writeBlock(
							datasetOut,
							attributes,
							LongArrayDataBlock(shardBlockSize, blockWithPosition._2(), data)
						)
				} else {
					val blockDims = Intervals.dimensionsAsIntArray(converted)
					val data = LongArray(Intervals.numElements(converted).toInt())
					val cursor = Views.flatIterable(converted).cursor()
					var i = 0
					cursor.forEach {
						val value = it.integerLong
						data[i++] = value
						if (blockIsEmpty && value != fillValue)
							blockIsEmpty = false
					}
					if (!blockIsEmpty)
						writer.writeBlock(
							datasetOut,
							attributes,
							LongArrayDataBlock(blockDims, blockWithPosition._2(), data)
						)
				}
			}
	}

	private fun <IN : IntegerType<IN>?, OUT : IntegerType<OUT>?> getAppropriateConverter(map: TLongLongMap?): Converter<IN, OUT> {
		LOG.trace { "Getting converter for map $map" }
		if (map == null || map.isEmpty) return Converter { s: IN, t: OUT -> t!!.setInteger(s!!.integerLong) }
		return Converter { s: IN, t: OUT ->
			val k = s!!.integerLong
			if (map.containsKey(k)) t!!.setInteger(map[k])
			else t!!.setInteger(k)
		}
	}

	private fun readAssignments(
		container: N5Reader,
		dataset: String
	): TLongLongMap {
		try {
			val data = openDatasetSafe(container, dataset)
			val keys = LongArray(data.dimension(0).toInt())
			val values = LongArray(keys.size)
			LOG.debug { "Found ${keys.size} assignments" }
			val keyCursor = Views.flatIterable(Views.hyperSlice(data, 1, 0L)).cursor()
			val valueCursor = Views.flatIterable(Views.hyperSlice(data, 1, 1L)).cursor()
			for (i in keys.indices) {
				keys[i] = keyCursor.next().integerLong
				values[i] = valueCursor.next().integerLong
			}
			return TLongLongHashMap(keys, values)
		} catch (e: IOException) {
			LOG.debug(e) { "Exception while trying to return initial lut from N5" }
			LOG.info { "Unable to read initial lut from $dataset in $container -- returning empty map" }
			return TLongLongHashMap()
		}
	}

	@Throws(IOException::class)
	private fun openDatasetSafe(
		reader: N5Reader,
		dataset: String
	): RandomAccessibleInterval<UnsignedLongType> {
		return if (DataType.UINT64 == reader.getDatasetAttributes(dataset).dataType) N5Utils.open(reader, dataset)
		else openAnyIntegerTypeAsUnsignedLongType(reader, dataset)
	}

	@Throws(IOException::class)
	private fun <T> openAnyIntegerTypeAsUnsignedLongType(
		reader: N5Reader,
		dataset: String
	): RandomAccessibleInterval<UnsignedLongType> where T : IntegerType<T>?, T : NativeType<T>? {
		val img: RandomAccessibleInterval<T> = N5Utils.open(reader, dataset)
		return Converters.convert(img, { s: T, t: UnsignedLongType -> t.setInteger(s!!.integerLong) }, UnsignedLongType())
	}

	class LookupPair private constructor(val key: Long, val value: Long) : Serializable {
		class Converter : CommandLine.ITypeConverter<LookupPair> {
			override fun convert(s: String): LookupPair {
				val split = s.split("=".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
				return LookupPair(split[0].toLong(), split[1].toLong())
			}
		}
	}

	private open class NoValidDatasetException : IOException {
		constructor(container: N5Reader, dataset: String?) : super(String.format("Unable to find valid data at %s in container %s", dataset, container))

		protected constructor(message: String) : super(message)
	}

	private class InvalidTypeException(dataType: DataType, isLabelMultiset: Boolean) : NoValidDatasetException(
		String.format(
			"Not a valid data type for conversion: (DataType=%s, isLabelMultiset=%s). Expected (DataType=%s, isLabelMultiset=true) or (DataType=any from %s, isLabelMultiset=false)",
			dataType,
			isLabelMultiset,
			DataType.UINT8,
			VALID_TYPES
		)
	)
}
