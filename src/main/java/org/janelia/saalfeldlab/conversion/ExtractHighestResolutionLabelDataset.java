package org.janelia.saalfeldlab.conversion;

import com.pivovarit.function.ThrowingConsumer;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.util.Grids;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.type.NativeType;
import net.imglib2.type.label.LabelMultisetType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.label.spark.N5Helpers;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5LabelMultisets;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.spark.supplier.N5ReaderSupplier;
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExtractHighestResolutionLabelDataset  {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static Set<DataType> VALID_TYPES = Stream
            .of(DataType.values())
            .filter(DataType.UINT64::equals)
            .filter(DataType.UINT32::equals)
            .filter(DataType.INT64::equals)
            .collect(Collectors.toSet());

    private static boolean isValidType(final DataType dataType) {
        return VALID_TYPES.contains(dataType);
    }

    private static Set<String> DATASET_ATTRIBUTES = Stream
            .of("dimensions", "blockSize", "compression", "dataType")
            .collect(Collectors.toSet());

    private static class LookupPair implements Serializable {

        public final long key;

        public final long value;

        private LookupPair(long key, long value) {
            this.key = key;
            this.value = value;
        }

        private static class Converter implements CommandLine.ITypeConverter<LookupPair> {

            @Override
            public LookupPair convert(String s) {
                final String[] split = s.split("=");
                return new LookupPair(Long.parseLong(split[0]), Long.parseLong(split[1]));
            }
        }
    }

    public static class Args implements Callable<Void>, Serializable {

        @CommandLine.Option(names = {"--input-container", "-i"}, required=true)
        String inputContainer = null;

        @CommandLine.Option(names = {"--input-dataset", "-I"}, required=true, description = "" +
                "Can be a Paintera dataset, multi-scale N5 group, or regular dataset. " +
                "Highest resolution dataset will be used for Paintera dataset (data/s0) and multi-scale group (s0).")
        String inputDataset = null;

        @CommandLine.Option(names = {"--output-container", "-o"}, required=true)
        String outputContainer = null;

        @CommandLine.Option(names = {"--output-dataset", "-O"}, required=false, description = "defaults to input dataset")
        String outputDataset = null;

        @CommandLine.Option(names = {"--block-size"}, required=false, split = ",", description = "" +
                "Block size for output dataset. Will default to block size of input dataset if not specified.")
        int[] blockSize = null;

        @CommandLine.Option(names = "--consider-fragment-segment-assignment", required = false, defaultValue = "false", description = "" +
                "Consider fragment-segment-assignment inside Paintera dataset. Will be ignored if not a Paintera dataset")
        Boolean considerFragmentSegmentAssignment = false;

        @CommandLine.Option(names = "--additional-assignment", split = ",", required = false, converter = LookupPair.Converter.class, description = "" +
                "Add additional lookup-values in the format `k=v'. Warning: Consistency with fragment-segment-assignment is not ensured.")
        LookupPair[] additionalAssignments = null;

        @Override
        public Void call() throws IOException {
            outputDataset = outputDataset == null ? inputDataset : outputDataset;

            if (inputContainer.equals(outputContainer) && inputDataset.equals(outputDataset)) {
                throw new IOException(String.format(
                        "Output dataset %s would overwrite input dataset %s in output container %s (same as input container %s)",
                        outputDataset,
                        inputDataset,
                        outputContainer,
                        inputContainer));
            }

            final SparkConf conf = new SparkConf().setAppName(MethodHandles.lookup().lookupClass().getName());

            final TLongLongMap assignment = new TLongLongHashMap();
            if (additionalAssignments != null)
                for (final LookupPair pair: additionalAssignments)
                    assignment.put(pair.key, pair.value);

            try (final JavaSparkContext sc = new JavaSparkContext(conf)) {
                extract(
                        sc,
                        () -> N5Helpers.n5Reader(inputContainer),
                        () -> N5Helpers.n5Writer(outputContainer),
                        inputDataset,
                        outputDataset,
                        blockSize,
                        considerFragmentSegmentAssignment != null && considerFragmentSegmentAssignment,
                        assignment);
            }

            return null;
        }
    }

    public static void main(String[] args) {
        CommandLine.call(new Args(), args);
    }

    public static <IN extends NativeType<IN> & IntegerType<IN>> void extract(
            final JavaSparkContext sc,
            final N5ReaderSupplier n5in,
            final N5WriterSupplier n5out,
            final String datasetIn,
            final String datasetOut,
            final int[] blockSizeOut,
            final boolean considerFragmentSegmentAssignment,
            final TLongLongMap assignment) throws IOException {
        ExtractHighestResolutionLabelDataset.<IN, UnsignedLongType>extract(
                sc,
                n5in,
                n5out,
                datasetIn,
                datasetOut,
                blockSizeOut,
                (Serializable & Supplier< UnsignedLongType >) UnsignedLongType::new,
                Collections.emptyMap(),
                considerFragmentSegmentAssignment,
                assignment);
    }

    public static <IN extends NativeType<IN> & IntegerType<IN>, OUT extends NativeType<OUT> & IntegerType<OUT>> void extract(
            final JavaSparkContext sc,
            final N5ReaderSupplier n5in,
            final N5WriterSupplier n5out,
            final String datasetIn,
            final String datasetOut,
            final int[] blockSizeOut,
            final Supplier<OUT> outputTypeSupplier,
            final Map<String, Object> additionalAttributes,
            final boolean considerFragmentSegmentAssignment,
            final TLongLongMap assignment
            ) throws IOException {

        if (!n5in.get().exists(datasetIn)) {
            throw new IOException(String.format("%s does not exist in container %s", datasetIn, n5in.get()));
        }

        if (!n5in.get().datasetExists(datasetIn)) {
            if (n5in.get().listAttributes(datasetIn).containsKey("painteraData")) {
                try {
                    final Map<String, Object> updatedAdditionalEntries = new HashMap<>(additionalAttributes);
                    Optional.ofNullable(n5in.get().getAttribute(datasetIn, "maxId", long.class)).ifPresent(id -> updatedAdditionalEntries.put("maxId", id));
                    if (considerFragmentSegmentAssignment) {
                        final TLongLongMap loadedAssignments = readAssignments(n5in.get(), datasetIn + "/fragment-segment-assignment");
                        loadedAssignments.putAll(assignment);
                        assignment.clear();
                        assignment.putAll(loadedAssignments);
                    }
                    extract(sc, n5in, n5out, datasetIn + "/data", datasetOut, blockSizeOut, outputTypeSupplier, updatedAdditionalEntries, considerFragmentSegmentAssignment, assignment);
                    return;
                } catch (final NoValidDatasetException e) {
                    throw new NoValidDatasetException(n5in.get(), datasetIn);
                }
            }
            else if (n5in.get().exists(datasetIn + "/s0")) {
                try {
                    extract(sc, n5in, n5out, datasetIn + "/s0", datasetOut, blockSizeOut, outputTypeSupplier, additionalAttributes, considerFragmentSegmentAssignment, assignment);
                    return;
                } catch (final NoValidDatasetException e) {
                    throw new NoValidDatasetException(n5in.get(), datasetIn);
                }
            }
            else
                throw new NoValidDatasetException(n5in.get(), datasetIn);
        }

        final boolean outputIsLabelMultiset = outputTypeSupplier.get() instanceof LabelMultisetType;

        final DatasetAttributes attributesIn = n5in.get().getDatasetAttributes(datasetIn);
        final long[] dimensions = attributesIn.getDimensions().clone();
        final int[] blockSize = blockSizeOut == null ? attributesIn.getBlockSize() : blockSizeOut;
        final DataType dataType = outputIsLabelMultiset
                ? DataType.UINT8
                : N5Utils.dataType(outputTypeSupplier.get());

        n5out.get().createDataset(datasetOut, dimensions, blockSize, dataType, new GzipCompression());
        final long[] keys = assignment.keys();
        final long[] values = assignment.values();

        // TODO automate copy of attributes if/when N5 separates attributes from dataset attributes
//        for (Map.Entry<String, Class<?>> entry :n5in.get().listAttributes(datasetIn).entrySet()) {
//            if (DATASET_ATTRIBUTES.contains(entry.getKey()))
//                continue;
//            try {
//                Object attr = n5in.get().getAttribute(datasetIn, entry.getKey(), entry.getValue());
//                LOG.debug("Copying attribute { {}: {} } of type {}", entry.getKey(), attr, entry.getValue());
//                n5out.get().setAttribute(datasetOut, entry.getKey(), attr);
//            } catch (IOException e) {
//                LOG.warn("Unable to copy attribute { {}: {} }", entry.getKey(), entry.getValue());
//                LOG.debug("Unable to copy attribute { {}: {} }", entry.getKey(), entry.getValue(), e);
//            }
//        }

        Optional
                .ofNullable(n5in.get().getAttribute(datasetIn, "resolution", double[].class))
                .ifPresent(ThrowingConsumer.unchecked(r -> n5out.get().setAttribute(datasetOut, "resolution", r)));

        Optional
                .ofNullable(n5in.get().getAttribute(datasetIn, "offset", double[].class))
                .ifPresent(ThrowingConsumer.unchecked(o -> n5out.get().setAttribute(datasetOut, "offset", o)));

        Optional
                .ofNullable(n5in.get().getAttribute(datasetIn, "maxId", long.class))
                .ifPresent(ThrowingConsumer.unchecked(id -> n5out.get().setAttribute(datasetOut, "maxId", id)));

        additionalAttributes.entrySet().forEach(ThrowingConsumer.unchecked(e -> n5out.get().setAttribute(datasetOut, e.getKey(), e.getValue())));

        try {
            n5out.get().setAttribute(datasetOut, N5LabelMultisets.LABEL_MULTISETTYPE_KEY, outputIsLabelMultiset);
        } catch (IOException e) {
            LOG.warn("Unable to write attribute { {}: {} }", N5LabelMultisets.LABEL_MULTISETTYPE_KEY, outputIsLabelMultiset);
            LOG.debug("Unable to write attribute { {}: {} }", N5LabelMultisets.LABEL_MULTISETTYPE_KEY, outputIsLabelMultiset, e);
        }
        final boolean isLabelMultiset = N5LabelMultisets.isLabelMultisetType(n5in.get(), datasetIn);

        if (!(DataType.UINT8.equals(attributesIn.getDataType()) && isLabelMultiset || isValidType(attributesIn.getDataType()) && !isLabelMultiset))
            throw new InvalidTypeException(attributesIn.getDataType(), isLabelMultiset);

        final List<Tuple2<Tuple2<long[], long[]>, long[]>> blocks = Grids
                .collectAllContainedIntervalsWithGridPositions(dimensions, blockSize)
                .stream()
                .map(p -> new Tuple2<>(new Tuple2<>(Intervals.minAsLongArray(p.getA()), Intervals.maxAsLongArray(p.getA())), p.getB()))
                .collect(Collectors.toList());

        sc
                .parallelize(blocks)
                .foreach(blockWithPosition -> {
                    RandomAccessibleInterval<IN> input = isLabelMultiset
                            ? (RandomAccessibleInterval) N5LabelMultisets.openLabelMultiset(n5in.get(), datasetIn)
                            : N5Utils.open(n5in.get(), datasetIn);
                    final RandomAccessibleInterval<IN> block = Views.interval(
                            input,
                            blockWithPosition._1()._1(),
                            blockWithPosition._1()._2());

                    final DatasetAttributes attributes = new DatasetAttributes(
                            dimensions,
                            blockSize,
                            N5Utils.dataType(outputTypeSupplier.get()),
                            new GzipCompression());

                    final RandomAccessibleInterval<OUT> converted = Converters.convert(block, getAppropriateConverter(new TLongLongHashMap(keys, values)), outputTypeSupplier.get());

                    N5Utils.saveBlock(converted, n5out.get(), datasetOut, attributes, blockWithPosition._2());

                });

    }

    private static <IN extends IntegerType<IN>, OUT extends IntegerType<OUT>> Converter<IN, OUT> getAppropriateConverter(final TLongLongMap map) {
        LOG.trace("Getting converter for map {}", map);
        if (map == null || map.isEmpty())
            return (s, t) -> t.setInteger(s.getIntegerLong());
        return (s, t) -> {
            final long k = s.getIntegerLong();
            if (map.containsKey(k))
                t.setInteger(map.get(k));
            else
                t.setInteger(k);
        };
    }

    private static class NoValidDatasetException extends IOException {

        private NoValidDatasetException(final N5Reader container, final String dataset) {
            super(String.format("Unable to find valid data at %s in container %s", dataset, container));
        }

        private NoValidDatasetException(final String message) {
            super(message);
        }

    }

    private static class InvalidTypeException extends NoValidDatasetException {

        private InvalidTypeException(final DataType dataType, final boolean isLabelMultiset) {

            super(String.format(
                    "Not a valid data type for conversion: (DataType=%s, isLabelMultiset=%s). Expected (DataType=%s, isLabelMultiset=true) or (DataType=any from %s, isLabelMultiset=false)",
                    dataType,
                    isLabelMultiset,
                    DataType.UINT8,
                    VALID_TYPES));

        }

    }

    private static TLongLongMap readAssignments(
            final N5Reader container,
            final String dataset) {
        try {
            RandomAccessibleInterval<UnsignedLongType> data = openDatasetSafe(container, dataset);
            final long[] keys = new long[(int) data.dimension(0)];
            final long[] values = new long[keys.length];
            LOG.debug("Found {} assignments", keys.length);
            final Cursor<UnsignedLongType> keyCursor = Views.flatIterable(Views.hyperSlice(data, 1, 0L)).cursor();
            final Cursor<UnsignedLongType> valueCursor = Views.flatIterable(Views.hyperSlice(data, 1, 1L)).cursor();
            for (int i = 0; i < keys.length; ++i) {
                keys[i] = keyCursor.next().getIntegerLong();
                values[i] = valueCursor.next().getIntegerLong();
            }
            return new TLongLongHashMap(keys, values);
        } catch (IOException e) {
            LOG.debug("Exception while trying to return initial lut from N5", e);
            LOG.info("Unable to read initial lut from {} in {} -- returning empty map", dataset, container);
            return new TLongLongHashMap();
        }
    }

    private static RandomAccessibleInterval<UnsignedLongType> openDatasetSafe(
            final N5Reader reader,
            final String dataset
    ) throws IOException {
        return DataType.UINT64.equals(reader.getDatasetAttributes(dataset).getDataType())
                ? N5Utils.open(reader, dataset)
                : openAnyIntegerTypeAsUnsignedLongType(reader, dataset);
    }

    private static <T extends IntegerType<T> & NativeType<T>> RandomAccessibleInterval<UnsignedLongType> openAnyIntegerTypeAsUnsignedLongType(
            final N5Reader reader,
            final String dataset
    ) throws IOException {
        final RandomAccessibleInterval<T> img = N5Utils.open(reader, dataset);
        return Converters.convert(img, (s, t) -> t.setInteger(s.getIntegerLong()), new UnsignedLongType());
    }

}
