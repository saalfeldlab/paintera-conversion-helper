package org.janelia.saalfeldlab.paintera.conversion;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.loops.LoopBuilder;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import org.janelia.saalfeldlab.label.spark.convert.ConvertToLabelMultisetType;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.imglib2.N5LabelMultisets;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Optional;

public class PainteraConvertTest {

    private static final long[] dimensions = {5, 4, 4};

    private static final int[] blockSize = {3, 3, 3};

    private static final String LABEL_SOURCE_DATASET = "volumes/labels-source";

    private static final RandomAccessibleInterval<UnsignedLongType> LABELS = ArrayImgs.unsignedLongs(
            new long[] {
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
            },
            dimensions);

    private final String tmpDir;

    private final N5Writer container;

    public PainteraConvertTest() throws IOException {
        this.tmpDir = Files.createTempDirectory("command-line-converter-test").toString();
        this.container = new N5FSWriter(tmpDir);
            container.createDataset(LABEL_SOURCE_DATASET, dimensions, blockSize, DataType.UINT64, new RawCompression());
        N5Utils.save(LABELS, container, LABEL_SOURCE_DATASET, blockSize, new RawCompression());
    }

    @SuppressWarnings("unchecked")
	@Test
    public void testWinnerTakesAll() throws IOException {
        final String labelTargetDataset = "volumes/labels-winner-takes-all";
        // TODO set spark master from outside, e.g. travis or in pom.xml
        System.setProperty("spark.master", "local[1]");
        PainteraConvert.main(new String[] {
                "to-paintera",
                "--container=" + tmpDir,
                "--output-container=" + tmpDir,
                "-d", LABEL_SOURCE_DATASET,
                "--type=label",
                "--target-dataset=" + labelTargetDataset,
                "--scale", "2",
                "--block-size=" + String.format("%s,%s,%s", blockSize[0], blockSize[1], blockSize[2]),
                "--winner-takes-all-downsampling"
        });

        Assert.assertTrue(container.exists(labelTargetDataset));
        Assert.assertTrue(container.exists(labelTargetDataset + "/data"));
        Assert.assertTrue(container.exists(labelTargetDataset + "/unique-labels"));
        Assert.assertTrue(container.exists(labelTargetDataset + "/label-to-block-mapping"));

        Assert.assertTrue(container.datasetExists(labelTargetDataset + "/data/s0"));
        Assert.assertTrue(container.datasetExists(labelTargetDataset + "/data/s1"));
        Assert.assertFalse(container.datasetExists(labelTargetDataset + "/data/s2"));

        Assert.assertEquals(5, (long) container.getAttribute(labelTargetDataset, "maxId", long.class));

        final DatasetAttributes attrsS0 = container.getDatasetAttributes(labelTargetDataset + "/data/s0");
        final DatasetAttributes attrsS1 = container.getDatasetAttributes(labelTargetDataset + "/data/s1");
        Assert.assertEquals(DataType.UINT64, attrsS0.getDataType());
        Assert.assertEquals(DataType.UINT64, attrsS1.getDataType());
        Assert.assertArrayEquals(blockSize, attrsS0.getBlockSize());
        Assert.assertArrayEquals(blockSize, attrsS1.getBlockSize());
        Assert.assertArrayEquals(dimensions, attrsS0.getDimensions());
        Assert.assertArrayEquals(Arrays.stream(dimensions).map(dimension -> dimension / 2).toArray(), attrsS1.getDimensions());

        LoopBuilder
                .setImages(LABELS, (RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(container, labelTargetDataset + "/data/s0"))
                .forEachPixel((e, a) -> Assert.assertTrue(e.valueEquals(a)));

        final RandomAccessibleInterval<UnsignedLongType> s1 = ArrayImgs.unsignedLongs(new long[] {
                5, 4,
                5, 4,

                4, 4,
                5, 4},
                attrsS1.getDimensions());

        LoopBuilder
                .setImages(s1, (RandomAccessibleInterval<UnsignedLongType>)N5Utils.open(container, labelTargetDataset + "/data/s1"))
                .forEachPixel((e, a) -> Assert.assertTrue(e.valueEquals(a)));
    }

    @Test
    public void testLabelMultisets() throws IOException {
        final String labelTargetDataset = "volumes/labels-converted";
        // TODO set spark master from outside, e.g. travis or in pom.xml
        System.setProperty("spark.master", "local[1]");
        PainteraConvert.main(new String[] {
                "to-paintera",
                "--container=" + tmpDir,
                "--output-container=" + tmpDir,
                "-d", LABEL_SOURCE_DATASET,
                "--type=label",
                "--target-dataset=" + labelTargetDataset,
                "--scale", "2",
                "--block-size=" + String.format("%s,%s,%s", blockSize[0], blockSize[1], blockSize[2])
        });

        Assert.assertTrue(container.exists(labelTargetDataset));
        Assert.assertTrue(container.exists(labelTargetDataset + "/data"));
        Assert.assertTrue(container.exists(labelTargetDataset + "/unique-labels"));
        Assert.assertTrue(container.exists(labelTargetDataset + "/label-to-block-mapping"));

        Assert.assertTrue(container.datasetExists(labelTargetDataset + "/data/s0"));
        Assert.assertTrue(container.datasetExists(labelTargetDataset + "/data/s1"));
        Assert.assertFalse(container.datasetExists(labelTargetDataset + "/data/s2"));

        Assert.assertEquals(5, (long) container.getAttribute(labelTargetDataset, "maxId", long.class));

        final DatasetAttributes attrsS0 = container.getDatasetAttributes(labelTargetDataset + "/data/s0");
        final DatasetAttributes attrsS1 = container.getDatasetAttributes(labelTargetDataset + "/data/s1");
        Assert.assertEquals(DataType.UINT8, attrsS0.getDataType());
        Assert.assertEquals(DataType.UINT8, attrsS1.getDataType());
        Assert.assertTrue(isLabelDataType(container, labelTargetDataset + "/data/s0"));
        Assert.assertTrue(isLabelDataType(container, labelTargetDataset + "/data/s1"));
        Assert.assertArrayEquals(blockSize, attrsS0.getBlockSize());
        Assert.assertArrayEquals(blockSize, attrsS1.getBlockSize());
        Assert.assertArrayEquals(dimensions, attrsS0.getDimensions());

        // FIXME: Should have the same dimensions as in the winner-takes-all case? Currently it's 1px more if input size is an odd number
        Assert.assertArrayEquals(Arrays.stream(dimensions).map(dimension -> dimension / 2 + (dimension % 2 != 0 ? 1 : 0)).toArray(), attrsS1.getDimensions());

        LoopBuilder
                .setImages(LABELS, N5LabelMultisets.openLabelMultiset(container, labelTargetDataset + "/data/s0"))
                .forEachPixel((e, a) ->
                	Assert.assertTrue(a.entrySet().size() == 1 && a.entrySet().iterator().next().getElement().id() == e.get())
            	);

        final RandomAccessibleInterval<UnsignedLongType> s1ArgMax = ArrayImgs.unsignedLongs(new long[] {
                5, 4, 4,
                5, 4, 1,

                4, 4, 4,
                5, 4, 1},
                attrsS1.getDimensions());

        LoopBuilder
		        .setImages(s1ArgMax, N5LabelMultisets.openLabelMultiset(container, labelTargetDataset + "/data/s1"))
		        .forEachPixel((e, a) ->
		        	Assert.assertEquals(e.get(), a.argMax()));

        /* Now test to-scalar, and ensure we can convert back. */
        final String scalarTargetDataset = "volumes/labels-back-to-scalar";
        PainteraConvert.main(new String[]{
                "to-scalar",
                "-i", tmpDir,
                "-I", labelTargetDataset,
                "-o", tmpDir,
                "-O", scalarTargetDataset});

        final CachedCellImg<UnsignedLongType, ?> toScalar = N5Utils.open(container, scalarTargetDataset);
        LoopBuilder
                .setImages(LABELS, toScalar)
                .forEachPixel((e, a) -> Assert.assertTrue(e.valueEquals(a)));
    }

    private static boolean isLabelDataType( final N5Reader n5Reader, final String fullSubGroupName ) throws IOException
    {
        switch ( n5Reader.getDatasetAttributes( fullSubGroupName ).getDataType() )
        {
            case UINT8: // label if LMT, otherwise raw
                return Optional.ofNullable( n5Reader.getAttribute( fullSubGroupName, ConvertToLabelMultisetType.LABEL_MULTISETTYPE_KEY, Boolean.class ) ).orElse( false );
            case UINT64:
            case UINT32:
            case INT64:
            case INT32:
                return true; // these are all label types

            default:
                return false;
        }
    }
}