package org.janelia.saalfeldlab.conversion

import net.imglib2.RandomAccessibleInterval
import net.imglib2.img.array.ArrayImgs
import net.imglib2.loops.LoopBuilder
import net.imglib2.type.label.LabelMultisetType
import net.imglib2.type.numeric.integer.UnsignedLongType
import org.janelia.saalfeldlab.conversion.PainteraConvert.Companion.main
import org.janelia.saalfeldlab.label.spark.convert.ConvertToLabelMultisetType
import org.janelia.saalfeldlab.n5.*
import org.janelia.saalfeldlab.n5.imglib2.N5LabelMultisets
import org.janelia.saalfeldlab.n5.imglib2.N5Utils
import org.janelia.saalfeldlab.n5.universe.N5Factory
import org.junit.Assert
import org.junit.Test
import java.io.IOException
import java.nio.file.Files
import java.util.Arrays
import java.util.Optional
import java.util.function.BiConsumer

class PainteraConvertTest {


	@Test
	fun testWinnerTakesAllN5() {
		testWinnerTakesAllWithExtension(".n5")
	}

	@Test
	fun testWinnerTakesAllH5() {
		testWinnerTakesAllWithExtension(".h5")
	}

	@Test
	fun testWinnerTakesAllZarr() {
		testWinnerTakesAllWithExtension(".zarr")
	}

	@Test
	fun testLabelMultisetsN5() {
		testLabelMultisetsWithExtension(".n5")
	}


	@Test
	fun testLabelMultisetsH5() {
		testLabelMultisetsWithExtension(".h5")
	}

	@Test
	fun testLabelMultisetsZarr() {
		testLabelMultisetsWithExtension(".zarr")
	}

	fun testWinnerTakesAllWithExtension(extension: String) {
		val scalarLabelsPath = "${Files.createTempDirectory("command-line-converter-test")}$extension"
		val scalarLabelsN5: N5Writer = N5Factory.createWriter(scalarLabelsPath)
		scalarLabelsN5.createDataset(LABEL_SOURCE_DATASET, dimensions, blockSize, DataType.UINT64, RawCompression())
		N5Utils.save(LABELS, scalarLabelsN5, LABEL_SOURCE_DATASET, blockSize, RawCompression())

		val painteraLabelsPath = "${Files.createTempDirectory("command-line-converter-test")}.n5"
		val painteraLabelsN5: N5Writer = N5Factory.createWriter(painteraLabelsPath)


		val labelTargetDataset = "volumes/labels-winner-takes-all"
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
				"--block-size=" + String.format("%s,%s,%s", blockSize[0], blockSize[1], blockSize[2]),
				"--winner-takes-all-downsampling"
			)
		)

		Assert.assertTrue(painteraLabelsN5.exists(labelTargetDataset))
		Assert.assertTrue(painteraLabelsN5.exists("$labelTargetDataset/data"))
		Assert.assertTrue(painteraLabelsN5.exists("$labelTargetDataset/unique-labels"))
		Assert.assertTrue(painteraLabelsN5.exists("$labelTargetDataset/label-to-block-mapping"))

		Assert.assertTrue(painteraLabelsN5.datasetExists("$labelTargetDataset/data/s0"))
		Assert.assertTrue(painteraLabelsN5.datasetExists("$labelTargetDataset/data/s1"))
		Assert.assertFalse(painteraLabelsN5.datasetExists("$labelTargetDataset/data/s2"))

		Assert.assertEquals(5, painteraLabelsN5.getAttribute(labelTargetDataset, "maxId", Long::class.javaPrimitiveType) as Long)

		val attrsS0 = painteraLabelsN5.getDatasetAttributes("$labelTargetDataset/data/s0")
		val attrsS1 = painteraLabelsN5.getDatasetAttributes("$labelTargetDataset/data/s1")
		Assert.assertEquals(DataType.UINT64, attrsS0.dataType)
		Assert.assertEquals(DataType.UINT64, attrsS1.dataType)
		Assert.assertArrayEquals(blockSize, attrsS0.blockSize)
		Assert.assertArrayEquals(blockSize, attrsS1.blockSize)
		Assert.assertArrayEquals(dimensions, attrsS0.dimensions)
		Assert.assertArrayEquals(Arrays.stream(dimensions).map { dimension: Long -> dimension / 2 }.toArray(), attrsS1.dimensions)

		LoopBuilder
			.setImages(LABELS, N5Utils.open<UnsignedLongType>(painteraLabelsN5, "$labelTargetDataset/data/s0"))
			.forEachPixel(BiConsumer { e: UnsignedLongType, a: UnsignedLongType -> Assert.assertTrue(e.valueEquals(a)) })

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
			.forEachPixel(BiConsumer { e: UnsignedLongType, a: UnsignedLongType -> Assert.assertTrue(e.valueEquals(a)) })
	}

	fun testLabelMultisetsWithExtension(extension: String) {

		val scalarLabelsPath = "${Files.createTempDirectory("command-line-converter-test")}$extension"
		val scalarLabelsN5: N5Writer = N5Factory.createWriter(scalarLabelsPath)
		scalarLabelsN5.createDataset(LABEL_SOURCE_DATASET, dimensions, blockSize, DataType.UINT64, RawCompression())
		N5Utils.save(LABELS, scalarLabelsN5, LABEL_SOURCE_DATASET, blockSize, RawCompression())

		val painteraLabelsPath = "${Files.createTempDirectory("command-line-converter-test")}.n5"
		val painteraLabelsN5: N5Writer = N5Factory.createWriter(painteraLabelsPath)

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

		Assert.assertTrue(painteraLabelsN5.exists(labelTargetDataset))
		Assert.assertTrue(painteraLabelsN5.exists("$labelTargetDataset/data"))
		Assert.assertTrue(painteraLabelsN5.exists("$labelTargetDataset/unique-labels"))
		Assert.assertTrue(painteraLabelsN5.exists("$labelTargetDataset/label-to-block-mapping"))

		Assert.assertTrue(painteraLabelsN5.datasetExists("$labelTargetDataset/data/s0"))
		Assert.assertTrue(painteraLabelsN5.datasetExists("$labelTargetDataset/data/s1"))
		Assert.assertFalse(painteraLabelsN5.datasetExists("$labelTargetDataset/data/s2"))

		Assert.assertEquals(5, painteraLabelsN5.getAttribute(labelTargetDataset, "maxId", Long::class.javaPrimitiveType) as Long)

		val attrsS0 = painteraLabelsN5.getDatasetAttributes("$labelTargetDataset/data/s0")
		val attrsS1 = painteraLabelsN5.getDatasetAttributes("$labelTargetDataset/data/s1")
		Assert.assertEquals(DataType.UINT8, attrsS0.dataType)
		Assert.assertEquals(DataType.UINT8, attrsS1.dataType)
		Assert.assertTrue(isLabelDataType(painteraLabelsN5, "$labelTargetDataset/data/s0"))
		Assert.assertTrue(isLabelDataType(painteraLabelsN5, "$labelTargetDataset/data/s1"))
		Assert.assertArrayEquals(blockSize, attrsS0.blockSize)
		Assert.assertArrayEquals(blockSize, attrsS1.blockSize)
		Assert.assertArrayEquals(dimensions, attrsS0.dimensions)

		// FIXME: Should have the same dimensions as in the winner-takes-all case? Currently it's 1px more if input size is an odd number
		Assert.assertArrayEquals(Arrays.stream(dimensions).map { dimension: Long -> dimension / 2 + (if (dimension % 2 != 0L) 1 else 0) }.toArray(), attrsS1.dimensions)

		LoopBuilder
			.setImages(LABELS, N5LabelMultisets.openLabelMultiset(painteraLabelsN5, "$labelTargetDataset/data/s0"))
			.forEachPixel(
				BiConsumer { e: UnsignedLongType, a: LabelMultisetType -> Assert.assertTrue(a.entrySet().size == 1 && a.entrySet().iterator().next().element.id() == e.get()) }
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
			.forEachPixel(BiConsumer { e: UnsignedLongType, a: LabelMultisetType -> Assert.assertEquals(e.get(), a.argMax()) })

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
			.forEachPixel(BiConsumer { e: UnsignedLongType, a: UnsignedLongType -> Assert.assertTrue(e.valueEquals(a)) })
	}

	companion object {
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

		@Throws(IOException::class)
		private fun isLabelDataType(n5Reader: N5Reader, fullSubGroupName: String): Boolean {
			return when (n5Reader.getDatasetAttributes(fullSubGroupName).dataType) {
				DataType.UINT8 -> Optional.ofNullable(n5Reader.getAttribute(fullSubGroupName, ConvertToLabelMultisetType.LABEL_MULTISETTYPE_KEY, Boolean::class.java)).orElse(false)
				DataType.UINT64, DataType.UINT32, DataType.INT64, DataType.INT32 -> true // these are all label types

				else -> false
			}
		}
	}
}
