package org.janelia.saalfeldlab.conversion

import gnu.trove.map.TLongLongMap
import gnu.trove.map.hash.TLongLongHashMap
import io.github.oshai.kotlinlogging.KotlinLogging
import net.imglib2.RandomAccessibleInterval
import net.imglib2.converter.Converters
import net.imglib2.img.array.ArrayImgs
import net.imglib2.loops.LoopBuilder
import net.imglib2.type.label.FromIntegerTypeConverter
import net.imglib2.type.label.LabelMultisetType
import net.imglib2.type.numeric.integer.UnsignedLongType
import net.imglib2.util.Intervals
import net.imglib2.view.Views
import org.apache.commons.io.FileUtils
import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.conversion.ExtractHighestResolutionLabelDataset.LookupPair
import org.janelia.saalfeldlab.conversion.ExtractHighestResolutionLabelDataset.extract
import org.janelia.saalfeldlab.conversion.to.newSparkConf
import org.janelia.saalfeldlab.n5.GzipCompression
import org.janelia.saalfeldlab.n5.N5Reader
import org.janelia.saalfeldlab.n5.imglib2.N5LabelMultisets
import org.janelia.saalfeldlab.n5.imglib2.N5Utils
import org.janelia.saalfeldlab.n5.spark.supplier.N5ReaderSupplier
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier
import org.janelia.saalfeldlab.n5.universe.N5Factory
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import picocli.CommandLine
import java.io.IOException
import java.io.Serializable
import java.nio.file.Files
import java.util.concurrent.Callable
import java.util.function.BiConsumer
import kotlin.Throws
import kotlin.test.assertEquals

class ExtractHighestResolutionLabelDatasetTest {

	@Test
	@Throws(IOException::class)
	fun testExtractHighestResolutionLabelDataset() {
		val conf = newSparkConf("local", "testExtractHighestResolutionLabelDataset")

		JavaSparkContext(conf).use { sc ->
			val tmpDir = Files.createTempDirectory("pch-extract-highest-resolution-")
			LOG.debug {
				FileUtils.forceDeleteOnExit(tmpDir.toFile())
				"Created tmp dir at $tmpDir"

			}

			val labelData: RandomAccessibleInterval<UnsignedLongType> = ArrayImgs.unsignedLongs(3, 4, 5)
			val c = Views.flatIterable(labelData).cursor()
			var i = 0
			while (c.hasNext()) {
				c.next().integer = i
				++i
			}

			val originalContainerPath = tmpDir.resolve("in.n5")
			val originalContainer = createWriter(originalContainerPath.toAbsolutePath().toString())
			val painteraDataset = "/my/dataset"
			val multiscaleGroup = String.format("%s/data", painteraDataset)
			val highestResolution = String.format("%s/s0", multiscaleGroup)
			originalContainer.createGroup(painteraDataset)
			originalContainer.createGroup(multiscaleGroup)

			val multisetData = Converters.convert(
				labelData,
				FromIntegerTypeConverter(),
				LabelMultisetType.singleEntryWithSingleOccurrence()
			)

			val resolution = doubleArrayOf(1.0, 2.0, 3.0)
			val offset = doubleArrayOf(2.0, 3.0, 4.0)
			val initialMaxId: Long = 0
			val editedMaxId = Intervals.numElements(labelData) - 1

			val inputBlockSize = intArrayOf(2, 2, 3)
			N5LabelMultisets.saveLabelMultiset(
				multisetData,
				originalContainer,
				highestResolution,
				inputBlockSize,
				GzipCompression()
			)

			originalContainer.setAttribute(painteraDataset, "painteraData", "")
			originalContainer.setAttribute(painteraDataset, "maxId", editedMaxId)
			originalContainer.setAttribute(highestResolution, "maxId", initialMaxId)
			originalContainer.setAttribute(highestResolution, "resolution", resolution)
			originalContainer.setAttribute(highestResolution, "offset", offset)

			val extensions = arrayOf("n5", "h5", "zarr")
			val inputDatasets = arrayOf(painteraDataset, multiscaleGroup, highestResolution)
			for (extension in extensions) {
				val outputContainerPath = tmpDir.resolve(String.format("out.%s", extension))
				val n5out: N5Reader = getWriter(outputContainerPath.toAbsolutePath().toString()).get()
				for (idx in inputDatasets.indices) {
					LOG.info { "Extracting paintera dataset ${originalContainerPath.toAbsolutePath()}:${inputDatasets[idx]} into $extension format" }
					extract(
						sc,
						getReader(originalContainerPath.toAbsolutePath().toString()),
						getWriter(outputContainerPath.toAbsolutePath().toString()),
						inputDatasets[idx],
						String.format("%d", idx),
						intArrayOf(4, 2, 3),
						false,
						TLongLongHashMap()
					)
					assertArrayEquals(resolution, n5out.getAttribute(String.format("%d", idx), "resolution", DoubleArray::class.java), 0.0)
					assertArrayEquals(offset, n5out.getAttribute(String.format("%d", idx), "offset", DoubleArray::class.java), 0.0)

					LoopBuilder.setImages(labelData, N5Utils.open<UnsignedLongType>(n5out, String.format("%d", idx)))
						.forEachPixel(BiConsumer { s: UnsignedLongType, t: UnsignedLongType ->
							LOG.debug { "Comparing $s and $t (actual)" }
							assertTrue(s.valueEquals(t))
						})
				}

				assertEquals(editedMaxId, n5out.getAttribute("0", "maxId", Long::class.javaPrimitiveType) as Long)
				assertEquals(initialMaxId, n5out.getAttribute("1", "maxId", Long::class.javaPrimitiveType) as Long)
				assertEquals(initialMaxId, n5out.getAttribute("2", "maxId", Long::class.javaPrimitiveType) as Long)
			}
		}
	}

	companion object {
		private val LOG = KotlinLogging.logger { }

		private fun getReader(container: String): N5ReaderSupplier {
			return N5ReaderSupplier { N5Factory.createReader(container) }
		}

		private fun getWriter(container: String): N5WriterSupplier {
			return N5WriterSupplier { N5Factory.createWriter(container) }
		}

		private fun testWithCLI(args: Array<String>) {
			CommandLine(TestArgs()).execute(*args)
		}

		private class TestArgs : Callable<Void?>, Serializable {
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

				val conf = newSparkConf("local")

				val assignment: TLongLongMap = TLongLongHashMap()
				if (additionalAssignments != null) for (pair in additionalAssignments!!) assignment.put(pair.key, pair.value)

				JavaSparkContext(conf).use { sc ->
					extract(
						sc,
						{ createReader(inputContainer!!) },
						{ createWriter(outputContainer!!) },
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
	}
}