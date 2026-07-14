package org.janelia.saalfeldlab.conversion

import com.google.gson.JsonParser
import net.imglib2.img.array.ArrayImgs
import net.imglib2.loops.LoopBuilder
import net.imglib2.type.numeric.integer.UnsignedLongType
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

/** `to-scalar --scale` builds a non-sharded multiscale pyramid for n5, zarr2 and zarr3, with the OME-NGFF version
 * appropriate to each format (zarr2 -> 0.4, n5/zarr3 -> 0.5). */
class ToScalarMultiFormatDownsampleTest {

	private val dims = longArrayOf(10, 7, 5)
	private val labels = ArrayImgs.unsignedLongs(LongArray((10 * 7 * 5)) { (it % 7 + 1).toLong() }, *dims)

	private fun writeInputN5(): String {
		val inputPath = "${Files.createTempDirectory("ts-in")}.n5"
		N5Utils.save(labels, createWriter(inputPath), "labels", intArrayOf(3, 3, 3), RawCompression())
		return inputPath
	}

	/** format, container extension, group-metadata file, expected OME-NGFF version */
	private data class Format(val name: String, val ext: String, val metaFile: String, val version: String)

	/** read back the multiscales version for the given format (v0.4 keeps it on the multiscale, v0.5 on `ome`) */
	private fun multiscaleVersion(container: String, fmt: Format): String {
		val root = JsonParser.parseString(File(container, fmt.metaFile).readText()).asJsonObject
		return when (fmt.name) {
			"ZARR2" -> root.getAsJsonArray("multiscales").single().asJsonObject.get("version").asString
			"ZARR3" -> root.getAsJsonObject("attributes").getAsJsonObject("ome").get("version").asString
			else -> root.getAsJsonObject("ome").get("version").asString
		}
	}

	private fun datasetPaths(container: String, fmt: Format): List<String> {
		val root = JsonParser.parseString(File(container, fmt.metaFile).readText()).asJsonObject
		val multiscales = when (fmt.name) {
			"ZARR2" -> root.getAsJsonArray("multiscales")
			"ZARR3" -> root.getAsJsonObject("attributes").getAsJsonObject("ome").getAsJsonArray("multiscales")
			else -> root.getAsJsonObject("ome").getAsJsonArray("multiscales")
		}
		return multiscales.single().asJsonObject.getAsJsonArray("datasets").map { it.asJsonObject.get("path").asString }
	}

	@Test
	fun `to-scalar --scale non-sharded pyramid for n5, zarr2, zarr3`() {
		val formats = listOf(
			Format("N5", ".n5", "scalar/attributes.json", "0.5"),
			Format("ZARR2", ".zarr", "scalar/.zattrs", "0.4"),
			Format("ZARR3", ".zarr", "scalar/zarr.json", "0.5")
		)

		for (fmt in formats) {
			val out = "${Files.createTempDirectory("ts-${fmt.name}")}${fmt.ext}"
			System.setProperty("spark.master", "local[1]")
			val code = CommandLine(PainteraConvert()).execute(
				"to-scalar",
				"-i", writeInputN5(),
				"-I", "labels",
				"-o", out,
				"-O", "scalar",
				"--output-format", fmt.name,
				"--block-size", "3,3,3",
				"--xyz-unit", "nanometer",
				"--scale", "2,2,2"
			)
			assertEquals(PainteraConvert.EXIT_CODE_SUCCESS, code, "${fmt.name}: exit code")

			val reader = createReader(out)
			val s0 = reader.getDatasetAttributes("scalar/s0")
			val s1 = reader.getDatasetAttributes("scalar/s1")
			assertFalse(s0.isSharded, "${fmt.name}: s0 must not be sharded")
			assertFalse(s1.isSharded, "${fmt.name}: s1 must not be sharded")
			assertEquals(listOf(5L, 3L, 2L), s1.dimensions.toList(), "${fmt.name}: s1 dims = s0 / 2")

			/* s0 matches the input */
			LoopBuilder.setImages(labels, N5Utils.open<UnsignedLongType>(reader, "scalar/s0"))
				.forEachPixel(BiConsumer { e: UnsignedLongType, a: UnsignedLongType -> assertTrue(e.valueEquals(a)) })

			/* multiscale metadata lists both levels with the appropriate version */
			assertEquals(listOf("s0", "s1"), datasetPaths(out, fmt), "${fmt.name}: multiscale datasets")
			assertEquals(fmt.version, multiscaleVersion(out, fmt), "${fmt.name}: OME-NGFF version")
		}
	}
}
