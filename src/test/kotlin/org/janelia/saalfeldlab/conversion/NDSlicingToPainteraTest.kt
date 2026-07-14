package org.janelia.saalfeldlab.conversion

import net.imglib2.img.array.ArrayImgs
import net.imglib2.type.label.LabelMultisetType
import net.imglib2.type.numeric.integer.UnsignedLongType
import org.janelia.saalfeldlab.conversion.PainteraConvert.Companion.main
import org.janelia.saalfeldlab.n5.RawCompression
import org.janelia.saalfeldlab.n5.imglib2.N5LabelMultisets
import org.janelia.saalfeldlab.n5.imglib2.N5Utils
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** to-paintera on a 4D label input with --slice-positions: produces a 3D Paintera LabelMultiset source from a single
 * channel slice, with the sliced 3D shape in data/s0 and the unique-labels index. */
class NDSlicingToPainteraTest {

	private val dims = longArrayOf(6, 7, 8, 3) // x, y, z, c
	private fun labelAt(p: LongArray) = 1 + p[0] + 10 * p[1] + 100 * p[2] + 1000 * p[3]

	@Test
	fun `4D label input sliced to 3D paintera LMT`() {
		val img = ArrayImgs.unsignedLongs(*dims)
		val cursor = img.localizingCursor()
		val p = LongArray(4)
		while (cursor.hasNext()) {
			cursor.fwd(); cursor.localize(p)
			cursor.get().set(labelAt(p))
		}

		val inputPath = "${Files.createTempDirectory("nd-in")}.n5"
		N5Utils.save(img, createWriter(inputPath), "labels", intArrayOf(3, 3, 3, 3), RawCompression())

		val outputPath = "${Files.createTempDirectory("nd-out")}.n5"
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
				"--slice-positions", "x,y,z,1", // axis0→x, axis1→y, axis2→z, channel axis fixed at 1
				"--dataset-resolution", "1,1,1"
			)
		)

		val reader = createReader(outputPath)
		assertTrue(reader.datasetExists("seg/data/s0"), "paintera s0 exists")
		val s0 = reader.getDatasetAttributes("seg/data/s0")
		assertEquals(listOf(6L, 7L, 8L), s0.dimensions.toList(), "s0 is the 3D sliced shape (x,y,z)")

		/* the indices are derived from s0, so they must carry the sliced shape too */
		assertTrue(reader.datasetExists("seg/unique-labels/s0"), "unique-labels index exists")
		assertEquals(listOf(6L, 7L, 8L), reader.getDatasetAttributes("seg/unique-labels/s0").dimensions.toList(), "unique-labels has sliced shape")

		/* the LMT round-trips the c=1 slice voxel-for-voxel */
		val lmt = N5LabelMultisets.openLabelMultiset(reader, "seg/data/s0")
		val ra = lmt.randomAccess()
		for (x in 0 until 6) for (y in 0 until 7) for (z in 0 until 8) {
			ra.setPosition(longArrayOf(x.toLong(), y.toLong(), z.toLong()))
			val expected = labelAt(longArrayOf(x.toLong(), y.toLong(), z.toLong(), 1))
			assertEquals(expected, ra.get().argMax(), "voxel ($x,$y,$z)")
		}
	}
}
