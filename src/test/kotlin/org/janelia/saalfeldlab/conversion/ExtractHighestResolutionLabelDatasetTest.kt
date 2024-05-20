package org.janelia.saalfeldlab.conversion

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
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.n5.GzipCompression
import org.janelia.saalfeldlab.n5.N5Reader
import org.janelia.saalfeldlab.n5.imglib2.N5LabelMultisets
import org.janelia.saalfeldlab.n5.imglib2.N5Utils
import org.janelia.saalfeldlab.n5.spark.supplier.N5ReaderSupplier
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier
import org.janelia.saalfeldlab.n5.universe.N5Factory
import org.junit.Assert
import org.junit.Test
import java.io.IOException
import java.nio.file.Files
import java.util.function.BiConsumer

class ExtractHighestResolutionLabelDatasetTest {
	@Test
	@Throws(IOException::class)
	fun testExtractHighestResolutionLabelDataset() {
		val conf = SparkConf()
			.setAppName("testExtractHighestResolutionLabelDataset") // TODO set this through maven?
			.setMaster("local")

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
			val originalContainer = N5Factory.createWriter(originalContainerPath.toAbsolutePath().toString())
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
					ExtractHighestResolutionLabelDataset.extract(
						sc,
						getReader(originalContainerPath.toAbsolutePath().toString()),
						getWriter(outputContainerPath.toAbsolutePath().toString()),
						inputDatasets[idx],
						String.format("%d", idx),
						intArrayOf(4, 2, 3),
						false,
						TLongLongHashMap()
					)
					Assert.assertArrayEquals(resolution, n5out.getAttribute(String.format("%d", idx), "resolution", DoubleArray::class.java), 0.0)
					Assert.assertArrayEquals(offset, n5out.getAttribute(String.format("%d", idx), "offset", DoubleArray::class.java), 0.0)

					LoopBuilder.setImages(labelData, N5Utils.open<UnsignedLongType>(n5out, String.format("%d", idx)))
						.forEachPixel(BiConsumer { s: UnsignedLongType, t: UnsignedLongType ->
							LOG.debug { "Comparing $s and $t (actual)" }
							Assert.assertTrue(s.valueEquals(t))
						})
				}

				Assert.assertEquals(editedMaxId, n5out.getAttribute("0", "maxId", Long::class.javaPrimitiveType) as Long)
				Assert.assertEquals(initialMaxId, n5out.getAttribute("1", "maxId", Long::class.javaPrimitiveType) as Long)
				Assert.assertEquals(initialMaxId, n5out.getAttribute("2", "maxId", Long::class.javaPrimitiveType) as Long)
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
	}
}