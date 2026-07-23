package org.janelia.saalfeldlab.conversion

import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import net.imglib2.RandomAccessibleInterval
import net.imglib2.img.array.ArrayImgs
import net.imglib2.loops.LoopBuilder
import net.imglib2.type.label.LabelMultisetType
import net.imglib2.type.numeric.integer.UnsignedLongType
import org.janelia.saalfeldlab.conversion.PainteraConvert.Companion.main
import org.janelia.saalfeldlab.label.spark.convert.ConvertToLabelMultisetType
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookup
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookupAdapter
import org.janelia.saalfeldlab.labels.blocks.n5.LabelBlockLookupFromN5Relative
import org.janelia.saalfeldlab.n5.DataType
import org.janelia.saalfeldlab.n5.DatasetAttributes
import org.janelia.saalfeldlab.n5.N5Reader
import org.janelia.saalfeldlab.n5.N5Writer
import org.janelia.saalfeldlab.n5.RawCompression
import org.janelia.saalfeldlab.n5.imglib2.N5LabelMultisets
import org.janelia.saalfeldlab.n5.imglib2.N5Utils
import org.janelia.saalfeldlab.n5.universe.N5Factory
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes
import org.junit.jupiter.api.Assertions.assertArrayEquals
import picocli.CommandLine
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.FieldSource
import java.nio.file.Files
import java.util.Arrays
import java.util.Optional
import java.util.function.BiConsumer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue



data class InputFormat(val label: String, val extension: String, val writePrefix: String, val sharded: Boolean = false) {
    override fun toString() = label
}

class PainteraConvertTest {

    @Test
    fun `default zarr2 separator`() {
        val zarrPath = "${Files.createTempDirectory("paintera_convert_default_sep")}.zarr"
        val zarr = createWriter("zarr2:" + zarrPath)
        var dataset = "dataset"
        zarr.createDataset(dataset, dimensions, blockSize, DataType.UINT64, RawCompression())
        val dimSep = zarr.getAttribute(dataset, "dimension_separator", String::class.java)
        assertEquals("/", dimSep)
    }

    /* create the scalar-label input in the requested format.
     * sharded zarr3 uses a shard grid of `shardSize` with inner chunks of `blockSize`.  */
    private fun writeScalarInput(format: InputFormat): String {
        val path = "${Files.createTempDirectory("command-line-converter-test")}${format.extension}"
        val writer = createWriter(format.writePrefix + path)
        val attributes = if (format.sharded)
            ZarrV3DatasetAttributes.Builder(LABELS.dimensionsAsLongArray(), DataType.UINT64)
                .blockSize(shardSize)
                .chunkSize(blockSize)
                .build()
        else
            DatasetAttributes.Builder(LABELS.dimensionsAsLongArray(), DataType.UINT64)
                .blockSize(blockSize)
                .build()
        writer.createDataset(LABEL_SOURCE_DATASET, attributes)
        N5Utils.saveBlock(LABELS, writer, LABEL_SOURCE_DATASET, attributes, longArrayOf(0, 0, 0))
        return path
    }

    @Test
    fun `to-paintera writes the labelBlockLookup parent attribute`() {
        val scalarLabelsPath = writeScalarInput(InputFormat("n5", ".n5", ""))
        val painteraLabelsPath = "${Files.createTempDirectory("labelblocklookup-attr-test")}.n5"
        createWriter(painteraLabelsPath)
        val group = "volumes/labels-with-lookup"

        System.setProperty("spark.master", "local[1]")
        main(
            arrayOf(
                "to-paintera",
                "--container=$scalarLabelsPath",
                "--output-container=$painteraLabelsPath",
                "-d", LABEL_SOURCE_DATASET,
                "--type=label",
                "--target-dataset=$group",
                "--block-size=" + String.format("%s,%s,%s", blockSize[0], blockSize[1], blockSize[2])
            )
        )

        /* read through a new reader */
        val factorNoCache = N5Factory()
            .options { opt -> opt.cacheAttributes(false) }
            .openReader(painteraLabelsPath)

        assertArrayEquals(intArrayOf(10000), factorNoCache.getDatasetAttributes("$group/label-to-block-mapping/s0").blockSize)

        /* "labelBlockLookup" attribute should be on the parent group */
        val raw = assertNotNull(factorNoCache.getAttribute(group, "labelBlockLookup", JsonObject::class.java))
        assertEquals(LabelBlockLookupFromN5Relative.LOOKUP_TYPE, raw.get("type").asString)
        assertEquals("label-to-block-mapping/s%d", raw.get("scaleDatasetPattern").asString)

        val lblGson = GsonBuilder()
            .registerTypeHierarchyAdapter(LabelBlockLookup::class.java, LabelBlockLookupAdapter.getJsonAdapter())
            .create()

        val lbl = assertNotNull(lblGson.fromJson(raw, LabelBlockLookup::class.java))
        assertTrue(lbl is LabelBlockLookupFromN5Relative)
    }

    @ParameterizedTest
    @FieldSource("conversionFormats")
    fun testWinnerTakesAll(format: InputFormat) {
        val scalarLabelsPath = writeScalarInput(format)

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
    @FieldSource("conversionFormats")
    fun testLabelMultisets(format: InputFormat) {

        val scalarLabelsPath = writeScalarInput(format)

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
                "-O", scalarTargetDataset,
                "--xyz-unit", "pixel"
            )
        )

        /* to-scalar output is an OME-NGFF multiscale group; the array lives at s0 */
        val toScalar = N5Utils.open<UnsignedLongType>(painteraLabelsN5, "$scalarTargetDataset/s0")
        LoopBuilder
            .setImages(LABELS, toScalar)
            .forEachPixel(BiConsumer { e: UnsignedLongType, a: UnsignedLongType -> assertTrue(e.valueEquals(a)) })
    }

    /* build a sharded zarr3 scalar label dataset: shard size [3,4,4], chunk size [3,2,2]  */
    private fun shardedZarr3Input(): String {
        val inputPath = "${Files.createTempDirectory("sharded-input")}.zarr"
        val inputWriter = createWriter("zarr3:$inputPath")
        val inputAttrs = ZarrV3DatasetAttributes(dimensions, intArrayOf(3, 4, 4), intArrayOf(3, 2, 2), DataType.UINT64)
        inputWriter.createDataset(LABEL_SOURCE_DATASET, inputAttrs)
        N5Utils.saveBlock(LABELS, inputWriter, LABEL_SOURCE_DATASET, inputAttrs, longArrayOf(0, 0, 0))
        return inputPath
    }

    @Test
    fun `sharded zarr3 scalar input to paintera, configurable output block size`() {
        val inputPath = shardedZarr3Input()
        /* output block size differs from both the input shard [3,4,4] and inner block [3,2,2] */
        val outBlock = intArrayOf(2, 2, 2)
        val painteraPath = "${Files.createTempDirectory("sharded-out")}.n5"
        val painteraN5: N5Writer = createWriter(painteraPath)
        val target = "volumes/labels-from-sharded"
        System.setProperty("spark.master", "local[1]")
        main(
            arrayOf(
                "to-paintera",
                "--container=$inputPath",
                "--output-container=$painteraPath",
                "-d", LABEL_SOURCE_DATASET,
                "--type=label",
                "--target-dataset=$target",
                "--winner-takes-all-downsampling",
                "--block-size=${outBlock[0]},${outBlock[1]},${outBlock[2]}"
            )
        )

        val attrsS0 = painteraN5.getDatasetAttributes("$target/data/s0")
        assertEquals(DataType.UINT64, attrsS0.dataType)
        /* the configurable output block size is honored regardless of the sharded input shape */
        assertEquals(outBlock.toList(), attrsS0.blockSize.toList())
        assertEquals(dimensions.toList(), attrsS0.dimensions.toList())
        LoopBuilder.setImages(LABELS, N5Utils.open<UnsignedLongType>(painteraN5, "$target/data/s0"))
            .forEachPixel(BiConsumer { e: UnsignedLongType, a: UnsignedLongType -> assertTrue(e.valueEquals(a)) })
    }

    @Test
    fun `sharded zarr3 scalar input to paintera label multiset`() {
        val inputPath = shardedZarr3Input()
        val painteraPath = "${Files.createTempDirectory("sharded-ms-out")}.n5"
        val painteraN5: N5Writer = createWriter(painteraPath)
        val target = "volumes/labels-ms-from-sharded"
        System.setProperty("spark.master", "local[1]")
        main(
            arrayOf(
                "to-paintera",
                "--container=$inputPath",
                "--output-container=$painteraPath",
                "-d", LABEL_SOURCE_DATASET,
                "--type=label",
                "--target-dataset=$target",
                "--block-size=2,2,2"
            )
        )

        val attrsS0 = painteraN5.getDatasetAttributes("$target/data/s0")
        assertEquals(DataType.UINT8, attrsS0.dataType)
        LoopBuilder.setImages(LABELS, N5LabelMultisets.openLabelMultiset(painteraN5, "$target/data/s0"))
            .forEachPixel(BiConsumer { e: UnsignedLongType, a: LabelMultisetType ->
                assertTrue(a.entrySet().size == 1 && a.entrySet().iterator().next().element.id() == e.get())
            })
    }

    /* an output container without a recognizable extension used to be created in the N5Factory
     * default format (zarr3), which cannot hold the varlen label multiset blocks */
    @Test
    fun `ambiguous output container should be n5`() {
        val inputPath = "${Files.createTempDirectory("no-ext-in")}.n5"
        N5Utils.save(LABELS, createWriter(inputPath), LABEL_SOURCE_DATASET, blockSize, RawCompression())
        val painteraPath = Files.createTempDirectory("no-ext-out").resolve("data").toString()
        val target = "volumes/labels"
        System.setProperty("spark.master", "local[1]")
        main(
            arrayOf(
                "to-paintera",
                "--container=$inputPath",
                "--output-container=$painteraPath",
                "-d", LABEL_SOURCE_DATASET,
                "--type=label",
                "--target-dataset=$target",
                "--block-size=2,2,2"
            )
        )

        assertTrue(Files.exists(java.nio.file.Paths.get(painteraPath, "attributes.json")))
        val painteraN5 = createWriter("n5:$painteraPath")
        assertEquals(DataType.UINT8, painteraN5.getDatasetAttributes("$target/data/s0").dataType)

        /* the unique-label extraction reads the multiset back; it threw NegativeArraySizeException on zarr3 */
        assertTrue(painteraN5.datasetExists("$target/unique-labels/s0"))
        LoopBuilder.setImages(LABELS, N5LabelMultisets.openLabelMultiset(painteraN5, "$target/data/s0"))
            .forEachPixel(BiConsumer { e: UnsignedLongType, a: LabelMultisetType ->
                assertTrue(a.entrySet().size == 1 && a.entrySet().iterator().next().element.id() == e.get())
            })
    }

    @Test
    fun `to-paintera only supports n5 output format`() {
        val inputPath = "${Files.createTempDirectory("guard-in")}.n5"
        N5Utils.save(LABELS, createWriter(inputPath), LABEL_SOURCE_DATASET, blockSize, RawCompression())
        val commonArgs = arrayOf(
            "to-paintera",
            "--container=$inputPath",
            "-d",
            LABEL_SOURCE_DATASET,
            "--type=label",
            "--target-dataset=out",
            "--winner-takes-all-downsampling",
            "--block-size=3,3,3"
        )

        /* explicit --output-format=ZARR3 */
        val explicit = CommandLine(PainteraConvert()).execute(
            *commonArgs, "--output-container=${Files.createTempDirectory("guard-out")}.n5", "--output-format=ZARR3"
        )
        assertEquals(exitCodes.INVALID_OUTPUT_CONTAINER, explicit)

        /* inferred from a .zarr output container */
        val inferred = CommandLine(PainteraConvert()).execute(
            *commonArgs, "--output-container=${Files.createTempDirectory("guard-out")}.zarr"
        )
        assertEquals(exitCodes.INVALID_OUTPUT_CONTAINER, inferred)
    }

    companion object {

        private val conversionFormats = arrayOf(
            InputFormat("n5", ".n5", ""),
            InputFormat("h5", ".h5", ""),
            InputFormat("zarr2", ".zarr", "zarr2:"),
            InputFormat("zarr3", ".zarr", "zarr3:"),
            InputFormat("zarr3-sharded", ".zarr", "zarr3:", sharded = true)
        )

        private val dimensions = longArrayOf(5, 4, 4)

        private val blockSize = intArrayOf(3, 3, 3)
        private val shardSize = intArrayOf(6, 6, 6)

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
