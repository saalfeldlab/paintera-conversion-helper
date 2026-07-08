package org.janelia.saalfeldlab.conversion

import com.google.gson.JsonParser
import net.imglib2.img.array.ArrayImgs
import net.imglib2.loops.LoopBuilder
import net.imglib2.type.numeric.integer.UnsignedLongType
import net.imglib2.view.Views
import org.janelia.saalfeldlab.n5.RawCompression
import org.janelia.saalfeldlab.n5.imglib2.N5Utils
import picocli.CommandLine
import java.io.File
import java.nio.file.Files
import java.util.function.BiConsumer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ToScalarZarr3OutputTest {

	private val dims = longArrayOf(10, 7, 5)
	private val labels = ArrayImgs.unsignedLongs(LongArray((10 * 7 * 5)) { (it % 7 + 1).toLong() }, *dims)

	/* zarr output is an OME-Zarr multiscale group */
	private val outputGroup = "scalar"
	private val outputDataset = "$outputGroup/s0"
	private val outputUnit = "nanometer"

	private fun writeInputN5(): String {
		val inputPath = "${Files.createTempDirectory("ts-in")}.n5"
		N5Utils.save(labels, createWriter(inputPath), "labels", intArrayOf(3, 3, 3), RawCompression())
		return inputPath
	}

	private fun runToScalar(units: List<String> = listOf(outputUnit), vararg extra: String): String {
		val outputPath = "${Files.createTempDirectory("ts-out")}.zarr"
		System.setProperty("spark.master", "local[1]")
		val code = CommandLine(PainteraConvert()).execute(
			"to-scalar",
			"-i", writeInputN5(),
			"-I", "labels",
			"-o", outputPath,
			"-O", outputGroup,
			"--output-format", "ZARR3",
			"--block-size", "3,3,3",
			"--xyz-unit", units.joinToString(","),
			*extra
		)
		assertEquals(PainteraConvert.EXIT_CODE_SUCCESS, code)
		return outputPath
	}

	private fun assertRoundTrip(outputPath: String) {
		val reader = createReader(outputPath)
		LoopBuilder.setImages(labels, N5Utils.open<UnsignedLongType>(reader, outputDataset))
			.forEachPixel(BiConsumer { e: UnsignedLongType, a: UnsignedLongType -> assertTrue(e.valueEquals(a)) })
	}

	/* parse the group's OME-NGFF 0.5 multiscales metadata and assert its structure. `expectedXyzUnits` is the
	 * per-axis unit in x, y, z order; zarr stores axes in C-order so they appear reversed (z, y, x). */
	private fun assertOmeNgffV05(outputPath: String, expectedXyzUnits: List<String>) {
		val root = JsonParser.parseString(File(outputPath, "$outputGroup/zarr.json").readText()).asJsonObject
		val ome = root.getAsJsonObject("attributes").getAsJsonObject("ome")
		assertEquals("0.5", ome.get("version").asString, "OME-NGFF version")

		val multiscale = ome.getAsJsonArray("multiscales").single().asJsonObject
		val axes = multiscale.getAsJsonArray("axes").map { it.asJsonObject }
		assertEquals(listOf("z", "y", "x"), axes.map { it.get("name").asString }, "axis order (C-order for zarr)")
		assertEquals(listOf("space", "space", "space"), axes.map { it.get("type").asString }, "axis types")
		assertEquals(expectedXyzUnits.reversed(), axes.map { it.get("unit").asString }, "per-axis units")

		val datasets = multiscale.getAsJsonArray("datasets").map { it.asJsonObject }
		assertEquals(listOf("s0"), datasets.map { it.get("path").asString }, "single scale s0")
	}

	@Test
	fun `to-scalar non-sharded zarr3, cropped boundary chunks`() {
		val out = runToScalar()
		val attrs = createReader(out).getDatasetAttributes(outputDataset)
		assertFalse(attrs.isSharded)
		assertEquals(listOf(3, 3, 3), attrs.blockSize.toList())
		assertRoundTrip(out)
		assertOmeNgffV05(out, List(3) { outputUnit })
	}

	@Test
	fun `to-scalar sharded zarr3, partial multi-shard`() {
		val out = runToScalar(listOf(outputUnit), "--chunks-per-shard", "2,2,2")
		val attrs = createReader(out).getDatasetAttributes(outputDataset)
		assertTrue(attrs.isSharded)
		assertEquals(listOf(6, 6, 6), attrs.blockSize.toList())
		assertTrue(File(out, "$outputDataset/zarr.json").readText().contains("sharding_indexed"))
		assertRoundTrip(out)
		assertOmeNgffV05(out, List(3) { outputUnit })
	}

	@Test
	fun `to-scalar zarr3 one unit applies to all three axes`() {
		val out = runToScalar(listOf("micrometer"))
		assertOmeNgffV05(out, listOf("micrometer", "micrometer", "micrometer"))
	}

	@Test
	fun `to-scalar zarr3 distinct per-axis units map to the correct axis`() {
		/* three distinct units so a mis-ordering or a botched C-order reversal would fail */
		val out = runToScalar(listOf("nanometer", "micrometer", "millimeter"))
		assertOmeNgffV05(out, listOf("nanometer", "micrometer", "millimeter"))
	}

	@Test
	fun `to-scalar rejects xyz-unit with two values`() {
		val outputPath = "${Files.createTempDirectory("ts-badunit-out")}.zarr"
		System.setProperty("spark.master", "local[1]")
		val code = CommandLine(PainteraConvert()).execute(
			"to-scalar",
			"-i", writeInputN5(),
			"-I", "labels",
			"-o", outputPath,
			"-O", outputGroup,
			"--output-format", "ZARR3",
			"--block-size", "3,3,3",
			"--xyz-unit", "nanometer,micrometer"
		)
		assertEquals(exitCodes.INVALID_AXIS_UNIT, code)
	}

	@Test
	fun `to-scalar sharded zarr3 skips empty shards`() {
		/* sparse input: all fill-value except one voxel in the first shard */
		val sparseDims = longArrayOf(12, 7, 5)
		val sparseImg = ArrayImgs.unsignedLongs(LongArray((12 * 7 * 5)), *sparseDims)
		sparseImg.randomAccess().apply {
			setPosition(longArrayOf(0, 0, 0))
			get().set(5L)
		}

		val inputPath = "${Files.createTempDirectory("ts-sparse-in")}.n5"
		N5Utils.save(sparseImg, createWriter(inputPath), "labels", intArrayOf(3, 3, 3), RawCompression())

		val outputPath = "${Files.createTempDirectory("ts-sparse-out")}.zarr"
		System.setProperty("spark.master", "local[1]")
		val code = CommandLine(PainteraConvert()).execute(
			"to-scalar",
			"-i", inputPath,
			"-I", "labels",
			"-o", outputPath,
			"-O", outputGroup,
			"--output-format", "ZARR3",
			"--block-size", "3,3,3",
			"--chunks-per-shard", "2,2,2",
			"--xyz-unit", outputUnit
		)
		assertEquals(PainteraConvert.EXIT_CODE_SUCCESS, code)

		/* 2x2x1 = 4 shards, but only the one holding the non-zero voxel is written */
		val shardFiles = File(outputPath, outputDataset)
			.walkTopDown()
			.count { it.isFile && it.name != "zarr.json" }
		assertEquals(1, shardFiles)

		/* the 3 skipped shards read back as the fill value (0); the one voxel round-trips */
		val reader = createReader(outputPath)
		LoopBuilder.setImages(sparseImg, N5Utils.open<UnsignedLongType>(reader, outputDataset))
			.forEachPixel(BiConsumer { e: UnsignedLongType, a: UnsignedLongType -> assertTrue(e.valueEquals(a)) })
		assertOmeNgffV05(outputPath, List(3) { outputUnit })
	}

	@Test
	fun `to-scalar zarr3 --scale builds a sharded multiscale pyramid`() {
		val out = runToScalar(listOf(outputUnit), "--chunks-per-shard", "2,2,2", "--scale", "2,2,2")
		val reader = createReader(out)

		val s0 = reader.getDatasetAttributes("$outputGroup/s0")
		val s1 = reader.getDatasetAttributes("$outputGroup/s1")
		assertTrue(s0.isSharded, "s0 is sharded")
		assertTrue(s1.isSharded, "downsampled level is sharded")
		assertEquals(listOf(5L, 3L, 2L), s1.dimensions.toList(), "s1 dims = s0 dims / 2")
		assertEquals(listOf(6, 6, 6), s1.blockSize.toList(), "s1 shard = chunksPerShard * block")

		/* the multiscales metadata now lists s0 and s1, with s1 scaled 2x the base */
		val ome = JsonParser.parseString(File(out, "$outputGroup/zarr.json").readText()).asJsonObject
			.getAsJsonObject("attributes").getAsJsonObject("ome")
		val datasets = ome.getAsJsonArray("multiscales").single().asJsonObject.getAsJsonArray("datasets").map { it.asJsonObject }
		assertEquals(listOf("s0", "s1"), datasets.map { it.get("path").asString })
		val s1Scale = datasets[1].getAsJsonArray("coordinateTransformations")[0].asJsonObject.getAsJsonArray("scale").map { it.asDouble }
		assertEquals(listOf(2.0, 2.0, 2.0), s1Scale, "s1 scale is 2x the base resolution")

		/* s1 is a valid, non-empty label array */
		var nonZero = 0
		Views.iterable(N5Utils.open<UnsignedLongType>(reader, "$outputGroup/s1")).forEach { if (it.get() != 0L) nonZero++ }
		assertTrue(nonZero > 0, "downsampled level has label data")
	}
}
