package org.janelia.saalfeldlab.conversion

import net.imglib2.img.array.ArrayImgs
import net.imglib2.type.numeric.integer.UnsignedLongType
import net.imglib2.view.Views
import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.conversion.to.newSparkConf
import org.janelia.saalfeldlab.n5.DataType
import org.janelia.saalfeldlab.n5.GzipCompression
import org.janelia.saalfeldlab.n5.imglib2.N5Utils
import org.janelia.saalfeldlab.n5.spark.downsample.N5LabelDownsamplerSpark
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier
import java.io.Serializable
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class N5LabelDownsamplerShardingTest {

	/* the sharded downsample write path (raw full-shard writeBlock) must produce exactly the same data as the
	 * trusted non-sharded path (saveNonEmptyBlock). dims 14 -> s1 dims 7 with shard 6 gives a partial edge shard. */
	@Test
	fun `sharded downsample equals non-sharded and is actually sharded`() {
		val dims = longArrayOf(14, 14, 14)
		val chunk = intArrayOf(3, 3, 3)
		val img = ArrayImgs.unsignedLongs(LongArray(14 * 14 * 14) { ((it * 7) % 41).toLong() }, *dims)

		val path = "${Files.createTempDirectory("ds-shard")}.zarr"
		val writer = createWriter("zarr3:$path")
		writer.createGroup("g")
		/* non-sharded s0 is written correctly by the standard imglib2 path */
		N5Utils.save(img, writer, "g/s0", chunk, GzipCompression())

		JavaSparkContext(newSparkConf("downsample-sharding", "local[1]")).use { sc ->
			val supplier = object : N5WriterSupplier, Serializable { override fun get() = createWriter("zarr3:$path") }
			/* reference: unsharded downsample */
			N5LabelDownsamplerSpark.downsampleLabel<UnsignedLongType>(sc, supplier, "g/s0", "g/s1ref", intArrayOf(2, 2, 2), chunk, null, false)
			/* under test: sharded downsample (2 chunks per shard -> shard 6, partial at the edge) */
			N5LabelDownsamplerSpark.downsampleLabel<UnsignedLongType>(sc, supplier, "g/s0", "g/s1shard", intArrayOf(2, 2, 2), chunk, intArrayOf(2, 2, 2), false)
		}

		val reader = createReader(path)
		val refAttrs = reader.getDatasetAttributes("g/s1ref")
		val shardAttrs = reader.getDatasetAttributes("g/s1shard")
		assertFalse(refAttrs.isSharded, "reference level should be unsharded")
		assertTrue(shardAttrs.isSharded, "level under test should be sharded")
		assertEquals(listOf(6, 6, 6), shardAttrs.blockSize.toList(), "sharded block size is the shard size")

		val ref = N5Utils.open<UnsignedLongType>(reader, "g/s1ref")
		val shard = N5Utils.open<UnsignedLongType>(reader, "g/s1shard")
		var mismatches = 0
		val refCursor = Views.flatIterable(ref).cursor()
		val shardCursor = Views.flatIterable(shard).cursor()
		while (refCursor.hasNext()) if (refCursor.next().get() != shardCursor.next().get()) mismatches++
		assertEquals(0, mismatches, "sharded output must match the non-sharded downsample voxel-for-voxel")
	}
}
