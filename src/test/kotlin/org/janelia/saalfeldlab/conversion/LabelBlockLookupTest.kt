package org.janelia.saalfeldlab.conversion

import net.imglib2.img.array.ArrayImgs
import net.imglib2.util.Intervals
import org.janelia.saalfeldlab.conversion.PainteraConvert.Companion.main
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookupKey
import org.janelia.saalfeldlab.labels.blocks.n5.LabelBlockLookupFromN5Relative
import org.janelia.saalfeldlab.n5.RawCompression
import org.janelia.saalfeldlab.n5.imglib2.N5Utils
import java.io.File
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Distinct labels in distinct s0 blocks must land in the matching unique-labels blocks (and thus the label-block
 * lookup). Reproduces "only block 0 has data". */
class LabelBlockLookupTest {

	@Test
	fun `unique-labels captures the non-zero block`() {
		/* 2 blocks along x: block [0,0,0] all = 100, block [1,0,0] all = 200 */
		val dims = longArrayOf(6, 3, 3)
		val img = ArrayImgs.unsignedLongs(*dims)
		val cursor = img.localizingCursor()
		val p = LongArray(3)
		while (cursor.hasNext()) {
			cursor.fwd(); cursor.localize(p)
			cursor.get().set(if (p[0] < 3) 100L else 200L)
		}

		val inputPath = "${Files.createTempDirectory("lbl-in")}.n5"
		N5Utils.save(img, createWriter(inputPath), "labels", intArrayOf(3, 3, 3), RawCompression())

		val outputPath = "${Files.createTempDirectory("lbl-out")}.n5"
		System.setProperty("spark.master", "local[1]")
		main(
			arrayOf(
				"to-paintera",
				"--output-container=$outputPath",
				"--block-size=3,3,3",
				"--container=$inputPath",
				"-d", "labels",
				"--type=label",
				"--target-dataset=seg",
				"--dataset-resolution", "1,1,1"
			)
		)

		val reader = createReader(outputPath)
		val ul = "seg/unique-labels/s0"
		assertTrue(reader.datasetExists(ul), "unique-labels/s0 exists")
		val attrs = reader.getDatasetAttributes(ul)

		val block0 = reader.readBlock<LongArray>(ul, attrs, *longArrayOf(0, 0, 0))?.data?.toSet() ?: emptySet()
		val block1 = reader.readBlock<LongArray>(ul, attrs, *longArrayOf(1, 0, 0))?.data?.toSet() ?: emptySet()

		println("unique-labels block[0,0,0] = $block0")
		println("unique-labels block[1,0,0] = $block1")

		assertTrue(100L in block0, "block [0,0,0] should contain label 100, was $block0")
		assertTrue(200L in block1, "block [1,0,0] should contain label 200, was $block1")

		/* now the actual label-block lookup: 100 -> block [0,0,0], 200 -> block [1,0,0] (voxel min x=3) */
		val lookup = LabelBlockLookupFromN5Relative("label-to-block-mapping/s%d")
		lookup.setRelativeTo(createWriter(outputPath), "seg")
		val blocks100 = lookup.read(LabelBlockLookupKey(0, 100L))
		val blocks200 = lookup.read(LabelBlockLookupKey(0, 200L))
		println("lookup[100] = ${blocks100.map { Intervals.minAsLongArray(it).toList() }}")
		println("lookup[200] = ${blocks200.map { Intervals.minAsLongArray(it).toList() }}")

		assertTrue(blocks100.isNotEmpty(), "label 100 should map to a block")
		assertTrue(blocks200.isNotEmpty(), "label 200 should map to a block")
		assertEquals(listOf(0L, 0L, 0L), Intervals.minAsLongArray(blocks100.single()).toList(), "100 -> block at [0,0,0]")
		assertEquals(listOf(3L, 0L, 0L), Intervals.minAsLongArray(blocks200.single()).toList(), "200 -> block at [3,0,0]")
	}

	@Test
	fun `label-block lookup is correct after nD slicing`() {
		/* 4D [x=6,y=3,z=3,c=2]; c=0 slice: x<3 -> 100, x>=3 -> 200; c=1 slice: all 999 (must be dropped) */
		val dims = longArrayOf(6, 3, 3, 2)
		val img = ArrayImgs.unsignedLongs(*dims)
		val cursor = img.localizingCursor()
		val p = LongArray(4)
		while (cursor.hasNext()) {
			cursor.fwd(); cursor.localize(p)
			cursor.get().set(if (p[3] == 1L) 999L else if (p[0] < 3) 100L else 200L)
		}

		val inputPath = "${Files.createTempDirectory("lbl-nd-in")}.n5"
		N5Utils.save(img, createWriter(inputPath), "labels", intArrayOf(3, 3, 3, 1), RawCompression())

		val outputPath = "${Files.createTempDirectory("lbl-nd-out")}.n5"
		System.setProperty("spark.master", "local[1]")
		main(
			arrayOf(
				"to-paintera",
				"--output-container=$outputPath",
				"--block-size=3,3,3",
				"--container=$inputPath",
				"-d", "labels",
				"--type=label",
				"--target-dataset=seg",
				"--slice-positions", "x,y,z,0", // fix channel 0
				"--dataset-resolution", "1,1,1"
			)
		)

		val reader = createReader(outputPath)
		assertEquals(listOf(6L, 3L, 3L), reader.getDatasetAttributes("seg/data/s0").dimensions.toList(), "sliced s0 is 3D")

		/* the exact symptom from the cluster output: lookup blockSize must be the stepSize (10000), not 3 */
		val lbmBs = reader.getDatasetAttributes("seg/label-to-block-mapping/s0").blockSize.toList()
		println("sliced LBM blockSize = $lbmBs, chunks = ${File(outputPath, "seg/label-to-block-mapping/s0").listFiles()?.filter { it.name != "attributes.json" }?.map { it.name }}")
		assertEquals(listOf(10000), lbmBs, "sliced-path lookup blockSize should be 10000, not 3")

		val lookup = LabelBlockLookupFromN5Relative("label-to-block-mapping/s%d")
		lookup.setRelativeTo(createWriter(outputPath), "seg")
		val b100 = lookup.read(LabelBlockLookupKey(0, 100L))
		val b200 = lookup.read(LabelBlockLookupKey(0, 200L))
		val b999 = lookup.read(LabelBlockLookupKey(0, 999L))
		println("sliced lookup[100] = ${b100.map { Intervals.minAsLongArray(it).toList() }}")
		println("sliced lookup[200] = ${b200.map { Intervals.minAsLongArray(it).toList() }}")
		println("sliced lookup[999] = ${b999.map { Intervals.minAsLongArray(it).toList() }}")

		assertEquals(listOf(0L, 0L, 0L), Intervals.minAsLongArray(b100.single()).toList(), "100 -> block [0,0,0]")
		assertEquals(listOf(3L, 0L, 0L), Intervals.minAsLongArray(b200.single()).toList(), "200 -> block [3,0,0]")
		assertTrue(b999.isEmpty(), "999 lives only in c=1 and must not appear in the c=0 slice")
	}

	@Test
	fun `label-block lookup handles large label ids`() {
		/* real segmentation ids are large; block[0,0,0] -> 5, block[1,0,0] -> 2_000_000 (lookup-block 200, not 0) */
		val small = 5L
		val large = 2_000_000L
		val dims = longArrayOf(6, 3, 3)
		val img = ArrayImgs.unsignedLongs(*dims)
		val cursor = img.localizingCursor()
		val p = LongArray(3)
		while (cursor.hasNext()) {
			cursor.fwd(); cursor.localize(p)
			cursor.get().set(if (p[0] < 3) small else large)
		}

		val inputPath = "${Files.createTempDirectory("lbl-big-in")}.n5"
		N5Utils.save(img, createWriter(inputPath), "labels", intArrayOf(3, 3, 3), RawCompression())

		val outputPath = "${Files.createTempDirectory("lbl-big-out")}.n5"
		System.setProperty("spark.master", "local[1]")
		main(
			arrayOf(
				"to-paintera",
				"--output-container=$outputPath",
				"--block-size=3,3,3",
				"--container=$inputPath",
				"-d", "labels",
				"--type=label",
				"--target-dataset=seg",
				"--dataset-resolution", "1,1,1"
			)
		)

		val lbm = "seg/label-to-block-mapping/s0"
		println("LBM s0 blockSize = ${createReader(outputPath).getDatasetAttributes(lbm).blockSize.toList()}")
		println("LBM s0 on-disk chunks = ${File(outputPath, lbm).listFiles()?.filter { it.name != "attributes.json" }?.map { it.name }?.sorted()}")

		val lookup = LabelBlockLookupFromN5Relative("label-to-block-mapping/s%d")
		lookup.setRelativeTo(createWriter(outputPath), "seg")
		val bSmall = lookup.read(LabelBlockLookupKey(0, small))
		val bLarge = lookup.read(LabelBlockLookupKey(0, large))
		println("lookup[$small] = ${bSmall.map { Intervals.minAsLongArray(it).toList() }}")
		println("lookup[$large] = ${bLarge.map { Intervals.minAsLongArray(it).toList() }}")

		assertEquals(listOf(0L, 0L, 0L), Intervals.minAsLongArray(bSmall.single()).toList(), "$small -> block [0,0,0]")
		assertTrue(bLarge.isNotEmpty(), "large id $large should map to a block, not be empty")
		assertEquals(listOf(3L, 0L, 0L), Intervals.minAsLongArray(bLarge.single()).toList(), "$large -> block [3,0,0]")
	}

	@Test
	fun `reproduce user flags slice plus scale plus block32`() {
		/* 5D [x=64,y=32,z=16,c=1,t=11]; c=0,t=10 plane: x<32 -> 106, x>=32 -> 200; other t -> 999 */
		val dims = longArrayOf(64, 32, 16, 1, 11)
		val img = ArrayImgs.unsignedLongs(*dims)
		val cursor = img.localizingCursor()
		val p = LongArray(5)
		while (cursor.hasNext()) {
			cursor.fwd(); cursor.localize(p)
			cursor.get().set(if (p[4] != 10L || p[3] != 0L) 999L else if (p[0] < 32) 106L else 200L)
		}

		val inputPath = "${Files.createTempDirectory("repro-in")}.n5"
		N5Utils.save(img, createWriter(inputPath), "labels", intArrayOf(32, 32, 16, 1, 11), RawCompression())

		val outputPath = "${Files.createTempDirectory("repro-out")}.n5"
		System.setProperty("spark.master", "local[*]")
		main(
			arrayOf(
				"to-paintera",
				"--output-container=$outputPath",
				"--block-size=32,32,32",
				"--scale", "2,2,2", "2,2,2", "2,2,2", "2,2,2",
				"--container=$inputPath",
				"-d", "labels",
				"--type=label",
				"--target-dataset=seg",
				"--slice-positions", "x,y,z,0,10",
				"--dataset-resolution", "1,1,1"
			)
		)

		val reader = createReader(outputPath)
		val bs = reader.getDatasetAttributes("seg/label-to-block-mapping/s0").blockSize.toList()
		println("REPRO LBM blockSize = $bs")
		println("REPRO LBM chunks = ${File(outputPath, "seg/label-to-block-mapping/s0").listFiles()?.filter { it.name != "attributes.json" }?.map { it.name }?.sorted()}")
		val lookup = LabelBlockLookupFromN5Relative("label-to-block-mapping/s%d")
		lookup.setRelativeTo(createWriter(outputPath), "seg")
		println("REPRO lookup[106] = ${lookup.read(LabelBlockLookupKey(0, 106L)).map { Intervals.minAsLongArray(it).toList() }}")
		println("REPRO lookup[200] = ${lookup.read(LabelBlockLookupKey(0, 200L)).map { Intervals.minAsLongArray(it).toList() }}")
		assertEquals(listOf(10000), bs, "lookup blockSize should be 10000")
		assertTrue(lookup.read(LabelBlockLookupKey(0, 106L)).isNotEmpty(), "106 must resolve")
	}

	@Test
	fun `label-block lookup is populated at every scale level`() {
		/* two big blocks along x; convert WITH a downsample pyramid and check the lookup at each level */
		val dims = longArrayOf(12, 6, 6)
		val img = ArrayImgs.unsignedLongs(*dims)
		val cursor = img.localizingCursor()
		val p = LongArray(3)
		while (cursor.hasNext()) {
			cursor.fwd(); cursor.localize(p)
			cursor.get().set(if (p[0] < 6) 7L else 8L)
		}

		val inputPath = "${Files.createTempDirectory("lbl-ms-in")}.n5"
		N5Utils.save(img, createWriter(inputPath), "labels", intArrayOf(3, 3, 3), RawCompression())

		val outputPath = "${Files.createTempDirectory("lbl-ms-out")}.n5"
		System.setProperty("spark.master", "local[1]")
		main(
			arrayOf(
				"to-paintera",
				"--output-container=$outputPath",
				"--block-size=3,3,3",
				"--scale", "2,2,2", "2,2,2",
				"--container=$inputPath",
				"-d", "labels",
				"--type=label",
				"--target-dataset=seg",
				"--dataset-resolution", "1,1,1"
			)
		)

		val lookup = LabelBlockLookupFromN5Relative("label-to-block-mapping/s%d")
		lookup.setRelativeTo(createWriter(outputPath), "seg")
		for (level in 0..2) {
			val b7 = lookup.read(LabelBlockLookupKey(level, 7L))
			val b8 = lookup.read(LabelBlockLookupKey(level, 8L))
			println("level $level: lookup[7] = ${b7.map { Intervals.minAsLongArray(it).toList() }}, lookup[8] = ${b8.map { Intervals.minAsLongArray(it).toList() }}")
			assertTrue(b7.isNotEmpty(), "label 7 must have blocks at level $level")
			assertTrue(b8.isNotEmpty(), "label 8 must have blocks at level $level")
		}
	}
}
