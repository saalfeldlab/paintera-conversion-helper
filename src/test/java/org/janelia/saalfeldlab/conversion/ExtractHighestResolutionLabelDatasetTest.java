package org.janelia.saalfeldlab.conversion;

import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.loops.LoopBuilder;
import net.imglib2.type.label.FromIntegerTypeConverter;
import net.imglib2.type.label.LabelMultisetType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.label.spark.N5Helpers;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5LabelMultisets;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.supplier.N5ReaderSupplier;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;

public class ExtractHighestResolutionLabelDatasetTest {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testExtractHighestResolutionLabelDataset() throws IOException {

        final SparkConf conf = new SparkConf()
                .setAppName("testExtractHighestResolutionLabelDataset")
                // TODO set this through maven?
                .setMaster("local");

        try (final JavaSparkContext sc = new JavaSparkContext(conf)) {

            final Path tmpDir = Files.createTempDirectory("pch-extract-highest-resolution-");

            if (!LOG.isDebugEnabled())
                FileUtils.forceDeleteOnExit(tmpDir.toFile());
            LOG.debug("Created tmp dir at {}", tmpDir);
            final String tmpDirAbsPath = tmpDir.toAbsolutePath().toString();

            final RandomAccessibleInterval<UnsignedLongType> labelData = ArrayImgs.unsignedLongs(3, 4, 5);
            final Cursor<UnsignedLongType> c = Views.flatIterable(labelData).cursor();
            for (int i = 0; c.hasNext(); ++i)
                c.next().setInteger(i);

            final Path originalContainerPath = tmpDir.resolve("in.n5");
            final N5FSWriter originalContainer = new N5FSWriter(originalContainerPath.toAbsolutePath().toString());
            final String painteraDataset = "/my/dataset";
            final String multiscaleGroup = String.format("%s/data", painteraDataset);
            final String highestResolution = String.format("%s/s0", multiscaleGroup);
            originalContainer.createGroup(painteraDataset);
            originalContainer.createGroup(multiscaleGroup);

            final RandomAccessibleInterval<LabelMultisetType> multisetData = Converters.convert(
                    labelData,
                    new FromIntegerTypeConverter<>(),
                    FromIntegerTypeConverter.getAppropriateType());

            final double[] resolution = {1.0, 2.0, 3.0};
            final double[] offset = {2.0, 3.0, 4.0};
            final long initialMaxId = 0;
            final long editedMaxId = Intervals.numElements(labelData) - 1;

            final int[] inputBlockSize = {2, 2, 3};
            N5LabelMultisets.saveLabelMultiset(
                    multisetData,
                    originalContainer,
                    highestResolution,
                    inputBlockSize,
                    new GzipCompression());

            originalContainer.setAttribute(painteraDataset, "painteraData", "");
            originalContainer.setAttribute(painteraDataset, "maxId", editedMaxId);
            originalContainer.setAttribute(highestResolution, "maxId", initialMaxId);
            originalContainer.setAttribute(highestResolution, "resolution", resolution);
            originalContainer.setAttribute(highestResolution, "offset", offset);

            final String[] extensions = {"n5", "h5"};
            final String[] inputDatasets = {painteraDataset, multiscaleGroup, highestResolution};
            for (final String extension : extensions) {
                final Path outputContainerPath = tmpDir.resolve(String.format("out.%s", extension));
                final N5Reader n5out = getWriter(outputContainerPath.toAbsolutePath().toString()).get();
                for (int i = 0; i < inputDatasets.length; ++i ) {
                    LOG.info("Extracting paintera dataset {}:{} into {} format", originalContainerPath.toAbsolutePath(), inputDatasets[i], extension);
                    ExtractHighestResolutionLabelDataset.extract(
                            sc,
                            getReader(originalContainerPath.toAbsolutePath().toString()),
                            getWriter(outputContainerPath.toAbsolutePath().toString()),
                            inputDatasets[i],
                            String.format("%d", i),
                            new int[] {4, 2, 3},
                            false,
                            new TLongLongHashMap());
                    Assert.assertArrayEquals(resolution, n5out.getAttribute(String.format("%d", i), "resolution", double[].class), 0.0);
                    Assert.assertArrayEquals(offset, n5out.getAttribute(String.format("%d", i), "offset", double[].class), 0.0);

                    LoopBuilder
                            .setImages(labelData, N5Utils.<UnsignedLongType>open(n5out, String.format("%d", i)))
                            .forEachPixel((s, t) -> {
                                LOG.debug("Comparing {} and {} (actual)", s, t);
                                Assert.assertTrue(s.valueEquals(t));
                            });

                }

                Assert.assertEquals(editedMaxId, (long) n5out.getAttribute("0", "maxId", long.class));
                Assert.assertEquals(initialMaxId, (long) n5out.getAttribute("1", "maxId", long.class));
                Assert.assertEquals(initialMaxId, (long) n5out.getAttribute("2", "maxId", long.class));

            }
        }

    }

    private static N5ReaderSupplier getReader(final String container) {
        return () -> N5Helpers.n5Reader(container);
    }

    private static N5WriterSupplier getWriter(final String container) {
        return () -> N5Helpers.n5Writer(container);
    }


}