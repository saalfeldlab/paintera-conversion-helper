package org.janelia.saalfeldlab.conversion

import net.imglib2.RandomAccessibleInterval
import net.imglib2.img.array.ArrayImgs
import net.imglib2.loops.LoopBuilder
import net.imglib2.type.label.LabelMultisetType
import net.imglib2.type.numeric.integer.UnsignedLongType
import org.janelia.saalfeldlab.conversion.PainteraConvert.Companion.main
import org.janelia.saalfeldlab.label.spark.convert.ConvertToLabelMultisetType
import org.janelia.saalfeldlab.n5.DataType
import org.janelia.saalfeldlab.n5.N5Reader
import org.janelia.saalfeldlab.n5.N5Writer
import org.janelia.saalfeldlab.n5.RawCompression
import org.janelia.saalfeldlab.n5.imglib2.N5LabelMultisets
import org.janelia.saalfeldlab.n5.imglib2.N5Utils
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.FieldSource
import java.nio.file.Files
import java.util.Arrays
import java.util.Optional
import java.util.function.BiConsumer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue


class PainteraConvertTest {

	@Test
	fun `default zarr separator`() {
		val zarrPath = "${Files.createTempDirectory("paintera_convert_default_sep")}.zarr"
		val zarr = createWriter(zarrPath)
		var dataset = "dataset"
		zarr.createDataset(dataset, dimensions, blockSize, DataType.UINT64, RawCompression())
		val dimSep = zarr.getAttribute<String>(dataset, "dimension_separator", String::class.java)
		assertEquals("/", dimSep)
	}



	@ParameterizedTest
	@FieldSource("extensionParams")
	fun testWinnerTakesAll(extension: String) {
		val scalarLabelsPath = "${Files.createTempDirectory("command-line-converter-test")}$extension"
		val scalarLabelsN5: N5Writer = createWriter(scalarLabelsPath)
		scalarLabelsN5.createDataset(LABEL_SOURCE_DATASET, dimensions, blockSize, DataType.UINT64, RawCompression())
		N5Utils.save(LABELS, scalarLabelsN5, LABEL_SOURCE_DATASET, blockSize, RawCompression())

		val painteraLabelsPath = "${Files.createTempDirectory("command-line-converter-test")}.n5"
		val painteraLabelsN5: N5Writer = createWriter(painteraLabelsPath)


		val labelTargetDataset = "volumes/labels-winner-takes-all"
		// TODO set spark master from outside, e.g. CI or in pom.xml
		System.setProperty("spark.master", "local[1]")
		main(
			arrayOf(
				"to-paintera",
				"--container=$scalarLabelsPath",
				"--output-container=$painteraLabelsPath",
				"-d", LABEL_SOURCE_DATASET,
				"--type=label",
				"--target-dataset=$labelTargetDataset",
				"--scale", "2",
				"--block-size=" + String.format("%s,%s,%s", blockSize[0], blockSize[1], blockSize[2]),
				"--winner-takes-all-downsampling"
			)
		)

		assertTrue(painteraLabelsN5.exists(labelTargetDataset))
		assertTrue(painteraLabelsN5.exists("$labelTargetDataset/data"))
		assertTrue(painteraLabelsN5.exists("$labelTargetDataset/unique-labels"))
		assertTrue(painteraLabelsN5.exists("$labelTargetDataset/label-to-block-mapping"))

		assertTrue(painteraLabelsN5.datasetExists("$labelTargetDataset/data/s0"))
		assertTrue(painteraLabelsN5.datasetExists("$labelTargetDataset/data/s1"))
		assertFalse(painteraLabelsN5.datasetExists("$labelTargetDataset/data/s2"))


		assertEquals(5, painteraLabelsN5.getAttribute(labelTargetDataset, "maxId", Long::class.javaPrimitiveType) as Long)

		val attrsS0 = painteraLabelsN5.getDatasetAttributes("$labelTargetDataset/data/s0")
		val attrsS1 = painteraLabelsN5.getDatasetAttributes("$labelTargetDataset/data/s1")
		assertEquals(DataType.UINT64, attrsS0.dataType)
		assertEquals(DataType.UINT64, attrsS1.dataType)
		assertArrayEquals(blockSize, attrsS0.blockSize)
		assertArrayEquals(blockSize, attrsS1.blockSize)
		assertArrayEquals(dimensions, attrsS0.dimensions)
		assertArrayEquals(Arrays.stream(dimensions).map { dimension: Long -> dimension / 2 }.toArray(), attrsS1.dimensions)

		LoopBuilder
			.setImages(LABELS, N5Utils.open<UnsignedLongType>(painteraLabelsN5, "$labelTargetDataset/data/s0"))
			.forEachPixel(BiConsumer { e: UnsignedLongType, a: UnsignedLongType -> assertTrue(e.valueEquals(a)) })

		val s1: RandomAccessibleInterval<UnsignedLongType> = ArrayImgs.unsignedLongs(
			longArrayOf(
				5, 4,
				5, 4,

				4, 4,
				5, 4
			),
			*attrsS1.dimensions
		)

		LoopBuilder
			.setImages(s1, N5Utils.open<UnsignedLongType>(painteraLabelsN5, "$labelTargetDataset/data/s1"))
			.forEachPixel(BiConsumer { e: UnsignedLongType, a: UnsignedLongType -> assertTrue(e.valueEquals(a)) })
	}

	@ParameterizedTest
	@FieldSource("extensionParams")
	fun testLabelMultisets(extension: String) {

		val scalarLabelsPath = "${Files.createTempDirectory("command-line-converter-test")}$extension"
		val scalarLabelsN5: N5Writer = createWriter(scalarLabelsPath)
		scalarLabelsN5.createDataset(LABEL_SOURCE_DATASET, dimensions, blockSize, DataType.UINT64, RawCompression())
		N5Utils.save(LABELS, scalarLabelsN5, LABEL_SOURCE_DATASET, blockSize, RawCompression())

		val painteraLabelsPath = "${Files.createTempDirectory("command-line-converter-test")}.n5"
		val painteraLabelsN5: N5Writer = createWriter(painteraLabelsPath)

		val labelTargetDataset = "volumes/labels-converted"
		// TODO set spark master from outside, e.g. travis or in pom.xml
		System.setProperty("spark.master", "local[1]")
		main(
			arrayOf(
				"to-paintera",
				"--container=$scalarLabelsPath",
				"--output-container=$painteraLabelsPath",
				"-d", LABEL_SOURCE_DATASET,
				"--type=label",
				"--target-dataset=$labelTargetDataset",
				"--scale", "2",
				"--block-size=" + String.format("%s,%s,%s", blockSize[0], blockSize[1], blockSize[2])
			)
		)

		assertTrue(painteraLabelsN5.exists(labelTargetDataset))
		assertTrue(painteraLabelsN5.exists("$labelTargetDataset/data"))
		assertTrue(painteraLabelsN5.exists("$labelTargetDataset/unique-labels"))
		assertTrue(painteraLabelsN5.exists("$labelTargetDataset/label-to-block-mapping"))

		assertTrue(painteraLabelsN5.datasetExists("$labelTargetDataset/data/s0"))
		assertTrue(painteraLabelsN5.datasetExists("$labelTargetDataset/data/s1"))
		assertFalse(painteraLabelsN5.datasetExists("$labelTargetDataset/data/s2"))

		assertEquals(5, painteraLabelsN5.getAttribute(labelTargetDataset, "maxId", Long::class.javaPrimitiveType) as Long)

		val attrsS0 = painteraLabelsN5.getDatasetAttributes("$labelTargetDataset/data/s0")
		val attrsS1 = painteraLabelsN5.getDatasetAttributes("$labelTargetDataset/data/s1")
		assertEquals(DataType.UINT8, attrsS0.dataType)
		assertEquals(DataType.UINT8, attrsS1.dataType)
		assertTrue(isLabelDataType(painteraLabelsN5, "$labelTargetDataset/data/s0"))
		assertTrue(isLabelDataType(painteraLabelsN5, "$labelTargetDataset/data/s1"))
		assertArrayEquals(blockSize, attrsS0.blockSize)
		assertArrayEquals(blockSize, attrsS1.blockSize)
		assertArrayEquals(dimensions, attrsS0.dimensions)

		// FIXME: Should have the same dimensions as in the winner-takes-all case? Currently it's 1px more if input size is an odd number
		assertArrayEquals(Arrays.stream(dimensions).map { dimension: Long -> dimension / 2 + (if (dimension % 2 != 0L) 1 else 0) }.toArray(), attrsS1.dimensions)

		LoopBuilder
			.setImages(LABELS, N5LabelMultisets.openLabelMultiset(painteraLabelsN5, "$labelTargetDataset/data/s0"))
			.forEachPixel(
				BiConsumer { e: UnsignedLongType, a: LabelMultisetType -> assertTrue(a.entrySet().size == 1 && a.entrySet().iterator().next().element.id() == e.get()) }
			)

		val s1ArgMax: RandomAccessibleInterval<UnsignedLongType> = ArrayImgs.unsignedLongs(
			longArrayOf(
				5, 4, 4,
				5, 4, 1,

				4, 4, 4,
				5, 4, 1
			),
			*attrsS1.dimensions
		)

		LoopBuilder
			.setImages(s1ArgMax, N5LabelMultisets.openLabelMultiset(painteraLabelsN5, "$labelTargetDataset/data/s1"))
			.forEachPixel(BiConsumer { e: UnsignedLongType, a: LabelMultisetType -> assertEquals(e.get(), a.argMax()) })

		/* Now test to-scalar, and ensure we can convert back. */
		val scalarTargetDataset = "volumes/labels-back-to-scalar"
		main(
			arrayOf(
				"to-scalar",
				"-i", painteraLabelsPath,
				"-I", labelTargetDataset,
				"-o", painteraLabelsPath,
				"-O", scalarTargetDataset
			)
		)

		val toScalar = N5Utils.open<UnsignedLongType>(painteraLabelsN5, scalarTargetDataset)
		LoopBuilder
			.setImages(LABELS, toScalar)
			.forEachPixel(BiConsumer { e: UnsignedLongType, a: UnsignedLongType -> assertTrue(e.valueEquals(a)) })
	}

	companion object {

		private val extensionParams = arrayOf(".n5", ".h5", ".zarr")

		private val dimensions = longArrayOf(5, 4, 4)

		private val blockSize = intArrayOf(3, 3, 3)

		private const val LABEL_SOURCE_DATASET = "volumes/labels-source"

		private val LABELS: RandomAccessibleInterval<UnsignedLongType> = ArrayImgs.unsignedLongs(
			longArrayOf(
				5, 5, 5, 4, 4,
				5, 5, 4, 4, 4,
				5, 4, 4, 4, 4,
				5, 4, 4, 4, 1,

				5, 5, 4, 4, 4,
				5, 4, 4, 4, 4,
				5, 5, 4, 4, 4,
				5, 5, 5, 1, 1,

				4, 4, 4, 4, 4,
				4, 4, 4, 4, 4,
				5, 4, 4, 4, 4,
				5, 5, 5, 5, 1,

				4, 4, 4, 4, 4,
				4, 4, 4, 4, 4,
				5, 4, 4, 4, 4,
				5, 5, 5, 5, 1
			),
			*dimensions
		)

		private fun isLabelDataType(n5Reader: N5Reader, fullSubGroupName: String): Boolean {
			return when (n5Reader.getDatasetAttributes(fullSubGroupName).dataType) {
				DataType.UINT8 -> Optional.ofNullable(n5Reader.getAttribute(fullSubGroupName, ConvertToLabelMultisetType.LABEL_MULTISETTYPE_KEY, Boolean::class.java)).orElse(false)
				DataType.UINT64, DataType.UINT32, DataType.INT64, DataType.INT32 -> true // these are all label types

				else -> false
			}
		}
	}
}
