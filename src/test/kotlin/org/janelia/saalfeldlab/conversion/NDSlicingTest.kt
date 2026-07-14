package org.janelia.saalfeldlab.conversion

import net.imglib2.img.array.ArrayImgs
import net.imglib2.util.Intervals
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class NDSlicingTest {

	private val dims = longArrayOf(4, 5, 6, 2, 3)

	/* unique per-voxel data so an incorrect slice/permute is obvious */
	private fun encode(p: LongArray) = p[0] + 10 * p[1] + 100 * p[2] + 1000 * p[3] + 10000 * p[4]

	private fun fill5D() = ArrayImgs.unsignedLongs(*dims).also { img ->
		val cursor = img.localizingCursor()
		val p = LongArray(5)
		while (cursor.hasNext()) {
			cursor.fwd(); cursor.localize(p)
			cursor.get().set(encode(p))
		}
	}

	@Test
	fun `sliceTo3D lands the mapped voxels`() {
		val img = fill5D()
		/* axis0→z, axis1→y, axis2→x, axis3 sliced at 1, axis4 sliced at 2 */
		val spec = parseSlicePositions("z,y,x,1,2", dims)
		val sliced = sliceTo3D(img, spec)

		/* output is x,y,z = input dims 2,1,0 = 6,5,4 */
		assertEquals(listOf(6L, 5L, 4L), Intervals.dimensionsAsLongArray(sliced).toList(), "sliced dims are x,y,z")

		val ra = sliced.randomAccess()
		for (ox in 0 until 6) for (oy in 0 until 5) for (oz in 0 until 4) {
			ra.setPosition(longArrayOf(ox.toLong(), oy.toLong(), oz.toLong()))
			/* input coords: dim0=oz, dim1=oy, dim2=ox, dim3=1, dim4=2 */
			val expected = encode(longArrayOf(oz.toLong(), oy.toLong(), ox.toLong(), 1, 2))
			assertEquals(expected, ra.get().get(), "voxel ($ox,$oy,$oz)")
		}
	}

	@Test
	fun `parser resolves spatial dims and sliced axes`() {
		val spec = parseSlicePositions("z,y,x,1,2", dims)
		assertEquals(listOf(2, 1, 0), spec.spatialInputDims.toList(), "x,y,z came from dims 2,1,0")
		assertEquals(mapOf(3 to 1L, 4 to 2L), spec.slicedAt)
		assertEquals(listOf(6L, 5L, 4L), spec.outputDimensions.toList())
	}

	@Test
	fun `parser rejects bad specs`() {
		assertFailsWith<InvalidSlicePositions> { parseSlicePositions("z,y,x,1", dims) }    // token count != numDims
		assertFailsWith<InvalidSlicePositions> { parseSlicePositions("z,y,y,1,2", dims) }  // missing x, duplicate y
		assertFailsWith<InvalidSlicePositions> { parseSlicePositions("z,y,x,9,2", dims) }  // slice 9 out of bounds on dim 3 (size 2)
		assertFailsWith<InvalidSlicePositions> { parseSlicePositions("z,y,x,q,2", dims) }  // non-integer, non-axis token
	}
}
