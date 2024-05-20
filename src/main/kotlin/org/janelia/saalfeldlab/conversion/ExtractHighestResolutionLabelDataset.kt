package org.janelia.saalfeldlab.conversion

import com.pivovarit.function.ThrowingConsumer
import gnu.trove.map.TLongLongMap
import gnu.trove.map.hash.TLongLongHashMap
import io.github.oshai.kotlinlogging.KotlinLogging
import net.imglib2.Interval
import net.imglib2.RandomAccessibleInterval
import net.imglib2.algorithm.util.Grids
import net.imglib2.converter.Converter
import net.imglib2.converter.Converters
import net.imglib2.type.NativeType
import net.imglib2.type.label.LabelMultisetType
import net.imglib2.type.numeric.IntegerType
import net.imglib2.type.numeric.integer.UnsignedLongType
import net.imglib2.util.Intervals
import net.imglib2.util.Pair
import net.imglib2.view.Views
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.n5.DataType
import org.janelia.saalfeldlab.n5.DatasetAttributes
import org.janelia.saalfeldlab.n5.GzipCompression
import org.janelia.saalfeldlab.n5.N5Reader
import org.janelia.saalfeldlab.n5.imglib2.N5LabelMultisets
import org.janelia.saalfeldlab.n5.imglib2.N5Utils
import org.janelia.saalfeldlab.n5.spark.supplier.N5ReaderSupplier
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier
import org.janelia.saalfeldlab.n5.universe.N5Factory
import picocli.CommandLine
import scala.Tuple2
import java.io.IOException
import java.io.Serializable
import java.lang.invoke.MethodHandles
import java.util.Optional
import java.util.concurrent.Callable
import java.util.function.Supplier
import java.util.stream.Collectors
import java.util.stream.Stream

object ExtractHighestResolutionLabelDataset {
	private val LOG = KotlinLogging.logger { }

	private val VALID_TYPES: Set<DataType> = Stream
		.of(*DataType.entries.toTypedArray())
		.filter { other: DataType? -> DataType.UINT64 == other }
		.filter { other: DataType? -> DataType.UINT32 == other }
		.filter { other: DataType? -> DataType.INT64 == other }
		.collect(Collectors.toSet())

	private fun isValidType(dataType: DataType): Boolean {
		return VALID_TYPES.contains(dataType)
	}

	@JvmStatic
	fun main(args: Array<String>) {
		CommandLine(Args()).execute(*args)
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
		assignment: TLongLongMap
	) {
		extract(sc, n5in, n5out, datasetIn, datasetOut, blockSizeOut, considerFragmentSegmentAssignment, assignment)
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
		assignment: TLongLongMap
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
			assignment
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
		assignment: TLongLongMap
	) where IN : NativeType<IN>?, IN : IntegerType<IN>?, OUT : NativeType<OUT>?, OUT : IntegerType<OUT>? {
		if (!n5in.get().exists(datasetIn)) {
			throw IOException(String.format("%s does not exist in container %s", datasetIn, n5in.get()))
		}

		if (!n5in.get().datasetExists(datasetIn)) {
			if (n5in.get().listAttributes(datasetIn).containsKey("painteraData")) {
				try {
					val updatedAdditionalEntries: MutableMap<String?, Any> = HashMap(additionalAttributes)
					Optional.ofNullable(n5in.get().getAttribute(datasetIn, "maxId", Long::class.javaPrimitiveType)).ifPresent { id: Long -> updatedAdditionalEntries["maxId"] = id }
					if (considerFragmentSegmentAssignment) {
						val loadedAssignments = readAssignments(n5in.get(), "$datasetIn/fragment-segment-assignment")
						loadedAssignments.putAll(assignment)
						assignment.clear()
						assignment.putAll(loadedAssignments)
					}
					extract(
						sc, n5in, n5out, "$datasetIn/data", datasetOut, blockSizeOut, outputTypeSupplier, updatedAdditionalEntries,
						considerFragmentSegmentAssignment, assignment
					)
					return
				} catch (e: NoValidDatasetException) {
					throw NoValidDatasetException(n5in.get(), datasetIn)
				}
			} else if (n5in.get().exists("$datasetIn/s0")) {
				try {
					extract(
						sc, n5in, n5out, "$datasetIn/s0", datasetOut, blockSizeOut, outputTypeSupplier, additionalAttributes,
						considerFragmentSegmentAssignment, assignment
					)
					return
				} catch (e: NoValidDatasetException) {
					throw NoValidDatasetException(n5in.get(), datasetIn)
				}
			} else throw NoValidDatasetException(n5in.get(), datasetIn)
		}

		val outputIsLabelMultiset = outputTypeSupplier.get() is LabelMultisetType

		val attributesIn = n5in.get().getDatasetAttributes(datasetIn)
		val dimensions = attributesIn.dimensions.clone()
		val blockSize = blockSizeOut ?: attributesIn.blockSize
		val dataType = if (outputIsLabelMultiset
		) DataType.UINT8
		else N5Utils.dataType(outputTypeSupplier.get())

		n5out.get().createDataset(datasetOut, dimensions, blockSize, dataType, GzipCompression())
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
			.ofNullable(n5in.get().getAttribute(datasetIn, "resolution", DoubleArray::class.java))
			.ifPresent(ThrowingConsumer.unchecked { r: DoubleArray -> n5out.get().setAttribute(datasetOut, "resolution", r) })

		Optional
			.ofNullable(n5in.get().getAttribute(datasetIn, "offset", DoubleArray::class.java))
			.ifPresent(ThrowingConsumer.unchecked { o: DoubleArray -> n5out.get().setAttribute(datasetOut, "offset", o) })

		Optional
			.ofNullable(n5in.get().getAttribute(datasetIn, "maxId", Long::class.javaPrimitiveType))
			.ifPresent(ThrowingConsumer.unchecked { id: Long -> n5out.get().setAttribute(datasetOut, "maxId", id) })

		additionalAttributes.entries.forEach(ThrowingConsumer.unchecked { e: Map.Entry<String?, Any> -> n5out.get().setAttribute(datasetOut, e.key, e.value) })

		try {
			n5out.get().setAttribute(datasetOut, N5LabelMultisets.LABEL_MULTISETTYPE_KEY, outputIsLabelMultiset)
		} catch (e: IOException) {
			LOG.warn { "Unable to write attribute { ${N5LabelMultisets.LABEL_MULTISETTYPE_KEY}: $outputIsLabelMultiset }" }
			LOG.debug(e) { "Unable to write attribute { ${N5LabelMultisets.LABEL_MULTISETTYPE_KEY}: $outputIsLabelMultiset }" }
		}
		val isLabelMultiset = N5LabelMultisets.isLabelMultisetType(n5in.get(), datasetIn)

		if (!(DataType.UINT8 == attributesIn.dataType && isLabelMultiset || isValidType(attributesIn.dataType) && !isLabelMultiset)) throw InvalidTypeException(attributesIn.dataType, isLabelMultiset)

		val blocks: List<Tuple2<Tuple2<LongArray, LongArray>, LongArray>> = Grids
			.collectAllContainedIntervalsWithGridPositions(dimensions, blockSize)
			.stream()
			.map { p: Pair<Interval, LongArray> -> Tuple2(Tuple2(Intervals.minAsLongArray(p.a), Intervals.maxAsLongArray(p.a)), p.b) }
			.collect(Collectors.toList())

		sc
			.parallelize(blocks)
			.foreach { blockWithPosition: Tuple2<Tuple2<LongArray, LongArray>, LongArray> ->
				val input: RandomAccessibleInterval<IN> =
					if (isLabelMultiset) N5LabelMultisets.openLabelMultiset(n5in.get(), datasetIn) as RandomAccessibleInterval<IN>
					else N5Utils.open(n5in.get(), datasetIn)
				val block: RandomAccessibleInterval<IN> = Views.interval(
					input,
					blockWithPosition._1()._1(),
					blockWithPosition._1()._2()
				)

				val attributes = DatasetAttributes(
					dimensions,
					blockSize,
					N5Utils.dataType(outputTypeSupplier.get()),
					GzipCompression()
				)

				val converted = Converters.convert(
					block, getAppropriateConverter(TLongLongHashMap(keys, values)),
					outputTypeSupplier.get()
				)
				N5Utils.saveBlock(converted, n5out.get(), datasetOut, attributes, blockWithPosition._2())
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

	class Args : Callable<Void?>, Serializable {
		@CommandLine.Option(names = ["--input-container", "-i"], required = true)
		var inputContainer: String? = null

		@CommandLine.Option(
			names = ["--input-dataset", "-I"], required = true, description = ["" +
					"Can be a Paintera dataset, multi-scale N5 group, or regular dataset. " +
					"Highest resolution dataset will be used for Paintera dataset (data/s0) and multi-scale group (s0)."]
		)
		var inputDataset: String? = null

		@CommandLine.Option(names = ["--output-container", "-o"], required = true)
		var outputContainer: String? = null

		@CommandLine.Option(names = ["--output-dataset", "-O"], required = false, description = ["defaults to input dataset"])
		var outputDataset: String? = null

		@CommandLine.Option(
			names = ["--block-size"], required = false, split = ",", description = ["" +
					"Block size for output dataset. Will default to block size of input dataset if not specified."]
		)
		var blockSize: IntArray? = null

		@CommandLine.Option(
			names = ["--consider-fragment-segment-assignment"], required = false, defaultValue = "false", description = ["" +
					"Consider fragment-segment-assignment inside Paintera dataset. Will be ignored if not a Paintera dataset"]
		)
		var considerFragmentSegmentAssignment: Boolean? = false

		@CommandLine.Option(
			names = ["--additional-assignment"], split = ",", required = false, converter = [LookupPair.Converter::class], description = ["" +
					"Add additional lookup-values in the format `k=v'. Warning: Consistency with fragment-segment-assignment is not ensured."]
		)
		var additionalAssignments: Array<LookupPair>? = null

		@Throws(IOException::class)
		override fun call(): Void? {
			outputDataset = if (outputDataset == null) inputDataset else outputDataset

			if (inputContainer == outputContainer && inputDataset == outputDataset) {
				throw IOException(
					String.format(
						"Output dataset %s would overwrite input dataset %s in output container %s (same as input container %s)",
						outputDataset,
						inputDataset,
						outputContainer,
						inputContainer
					)
				)
			}

			val conf = SparkConf().setAppName(MethodHandles.lookup().lookupClass().name)

			val assignment: TLongLongMap = TLongLongHashMap()
			if (additionalAssignments != null) for (pair in additionalAssignments!!) assignment.put(pair.key, pair.value)

			JavaSparkContext(conf).use { sc ->
				extract(
					sc,
					{ N5Factory.createReader(inputContainer) },
					{ N5Factory.createWriter(outputContainer) },
					inputDataset,
					outputDataset,
					blockSize,
					considerFragmentSegmentAssignment ?: false,
					assignment
				)
			}
			return null
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
