package org.janelia.saalfeldlab.conversion;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.label.spark.N5Helpers;
import org.janelia.saalfeldlab.label.spark.convert.ConvertToLabelMultisetType;
import org.janelia.saalfeldlab.label.spark.downsample.SparkDownsampler;
import org.janelia.saalfeldlab.label.spark.exception.InputSameAsOutput;
import org.janelia.saalfeldlab.label.spark.exception.InvalidDataType;
import org.janelia.saalfeldlab.label.spark.exception.InvalidDataset;
import org.janelia.saalfeldlab.label.spark.exception.InvalidN5Container;
import org.janelia.saalfeldlab.label.spark.uniquelabels.ExtractUniqueLabelsPerBlock;
import org.janelia.saalfeldlab.label.spark.uniquelabels.LabelToBlockMapping;
import org.janelia.saalfeldlab.label.spark.uniquelabels.downsample.LabelListDownsampler;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.spark.N5ConvertSpark;
import org.janelia.saalfeldlab.n5.spark.downsample.N5DownsamplerSpark;
import org.janelia.saalfeldlab.n5.spark.downsample.N5LabelDownsamplerSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@Deprecated
public class CommandLineConverter
{
	private static final String DEPRECATION_MESSAGE = "The CommandLineConverter has been deprecated in favor of PainteraConvert. Please use PainteraConvert instead.";

	private static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	private static final String LABEL_BLOCK_LOOKUP_KEY = "labelBlockLookup";

	private static final String RAW_IDENTIFIER = "raw";

	private static final String LABEL_IDENTIFIER = "label";

	private static final String CHANNEL_IDENTIFIER = "channel";

	private static final String CHANNEL_AXIS_KEY = "channelAxis";

	private static final String CHANNEL_BLOCKSIZE_KEY = "channelBlockSize";

	private static final String RESOLUTION_KEY = "resolution";

	private static final String OFFSET_KEY = "offset";

	private static final GsonBuilder DEFAULT_BUILDER = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping();

	public static class CommandLineParameters
	{
		@Option( names = { "-d", "--dataset" },
				description = "Comma delimited description of dataset; <n5 root path>,<path/to/dataset>,<raw|label|channel[:channelAxis=<axis>][,optional name]" )
		private String[] datasets;

		@Option( names = { "-s", "--scale" }, arity = "1..*", description = "Factor by which to downscale the input image. Factors are relative to the previous level, not to level zero. Format either fx,fy,fz or f" )
		private String[] scales;

		@Option( names = { "-o", "--outputN5" }, required = true )
		private String outputN5;

		@Option( names = {"--blocksize", "-b"}, paramLabel = "BLOCK_SIZE", description = "block size for initial conversion in the format bx,by,bz or b for isotropic block size. Defaults to 64,64,64", split = ",")
		private int[] blockSize = null;

		@Option( names = { "--downsample-block-sizes" }, arity = "1..*", description = "Block size for each downscaled level in the format bx,by,bz or b for isotropic block size. Does not need to be specified for each scale level (defaults to previous level if not specified, or BLOCK_SIZE if not specified at all)")
		private String[] downsampleBlockSizes = null;

		@Option( names = { "-h", "--help" }, usageHelp = true, description = "display a help message" )
		private boolean helpRequested;

		@Option( names = { "-r", "--revert" }, description = "Reverts array attributes" )
		private boolean revert;

		@Option( names = { "--winner-takes-all-downsampling" }, description = "Use winner-takes-all-downsampling for labels" )
		private boolean winnerTakesAll;

		@Option( names = { "-c", "--convert-entire-container" }, description = "Convert entire container; auto-detect dataset types" )
		private String convertEntireContainer;

		@Option( names = { "--resolution" }, description = "Voxel size.", split = "," )
		private double[] resolution;

		@Option( names = { "--offset" }, description = "Offset in world coordinates.", split = "," )
		private double[] offset;

		@Option( names = { "--label-block-lookup-backend-n5" }, paramLabel = "BLOCK_SIZE", description = "Block size for n5 backend for label block lookup, defaults to 10000.", defaultValue = "10000" )
		private Integer labelBlockLookupN5BlockSize = 10000;

		@Option(names = {"--max-num-entries", "-m"}, arity = "1..*", description = "max number of entries for each label multiset at each scale. Pick lower number for higher scale levels")
		private int[] maxNumEntries;

		@Option( names =  "--use-file-based-label-block-lookup", description = "Use label-block-lookup with a single file instead of a sparse map backed by N5. Recommended only for small datasets" )
		private boolean useFileBasedLabelBlockLookup = false;
	}

	public static void main( final String[] args ) throws IOException, InvalidDataType, InvalidN5Container, InvalidDataset, InputSameAsOutput, ConverterException {
		run(args);
		System.err.println(DEPRECATION_MESSAGE);
	}

	public static void run( final String... args ) throws IOException, InvalidDataType, InvalidN5Container, InvalidDataset, InputSameAsOutput, ConverterException
	{
		final CommandLineParameters clp = new CommandLineParameters();
		final CommandLine cl = new CommandLine( clp );
		cl.parse( args );

		if ( cl.isUsageHelpRequested() )
		{
			cl.usage( System.out );
			return;
		}

		clp.scales = clp.scales == null ? new String[0] : clp.scales;
		clp.blockSize = clp.blockSize == null || clp.blockSize.length == 0 ? new int[] {64, 64, 64} : clp.blockSize.length == 3 ? clp.blockSize : new int[] {clp.blockSize[0], clp.blockSize[0], clp.blockSize[0]};
		clp.downsampleBlockSizes = clp.downsampleBlockSizes == null || clp.downsampleBlockSizes.length == 0
				? new String[] {String.join(",", IntStream.of(clp.blockSize).mapToObj(Integer::toString).toArray(String[]::new))}
				: clp.downsampleBlockSizes;
		clp.maxNumEntries = clp.maxNumEntries == null || clp.maxNumEntries.length == 0 ? new int[] {-1} : clp.maxNumEntries;

		final String[] formattedScales = ( clp.scales != null ? new String[ clp.scales.length ] : null );
		if ( formattedScales != null )
		{
			for ( int i = 0; i < clp.scales.length; ++i )
			{
				if ( clp.scales[ i ].split( "," ).length == 1 )
				{
					formattedScales[ i ] = String.join( ",", clp.scales[ i ], clp.scales[ i ], clp.scales[ i ] );
				}
				else
				{
					formattedScales[ i ] = clp.scales[ i ];
				}
			}
		}
		final int[][] scales = ( formattedScales != null ) ? Arrays
				.stream( formattedScales )
				.map( fs -> fs.split( "," ) )
				.map( s -> Arrays.stream( s ).mapToInt( Integer::parseInt ).toArray() )
				.toArray( int[][]::new ) : new int[][] {};

		final int[][] downsamplingBlockSizes = new int[clp.scales.length][];
		Arrays.setAll(downsamplingBlockSizes, level -> {
			if (level >= clp.downsampleBlockSizes.length)
			{
				return downsamplingBlockSizes[level - 1];
			}
			else
			{
				final String[] split = clp.downsampleBlockSizes[level].split(",");
				final Stream<String> stream = split.length == 3 ? Stream.of(split) : Stream.generate(() -> split[0]).limit(3);
				return stream.mapToInt(Integer::parseInt).toArray();
			}
		});
		int[] blockSize = clp.blockSize;
		final int[] maxNumEntriesArray = new int[scales.length];
		for ( int level = 0; level < maxNumEntriesArray.length; ++level )
			maxNumEntriesArray[ level ] = level < clp.maxNumEntries.length ? clp.maxNumEntries[ level ] : maxNumEntriesArray[ level - 1 ];

		LOG.debug("Got initial block size {}", blockSize);
		for ( int level = 0; level < downsamplingBlockSizes.length; ++level )
			LOG.debug("Downsampling block size for level {}: {}", level, downsamplingBlockSizes[level]);

		final Optional< double[] > resolution = Optional.ofNullable( clp.resolution );
		final Optional< double[] > offset = Optional.ofNullable( clp.offset );

		final SparkConf conf = new SparkConf().setAppName(MethodHandles.lookup().lookupClass().getName());

		// do not use n5 if file based label block lookup is requested
		final Optional< Integer > labelBlockLookupN5BlockSize = Optional
				.ofNullable( clp.labelBlockLookupN5BlockSize )
				.filter( i -> !clp.useFileBasedLabelBlockLookup );

		try(final JavaSparkContext sc = new JavaSparkContext(conf)) {

			if (clp.convertEntireContainer != null) {
				final N5Reader n5Reader = N5Helpers.n5Reader(clp.convertEntireContainer);
				convertAll(
						clp.convertEntireContainer,
						"",
						sc,
						n5Reader,
						blockSize,
						downsamplingBlockSizes,
						maxNumEntriesArray,
						scales,
						clp.outputN5,
						clp.revert,
						clp.winnerTakesAll,
						labelBlockLookupN5BlockSize,
						resolution,
						offset);
			} else {
				for (int i = 0; i < clp.datasets.length; ++i) {
					final String[] datasetInfo = clp.datasets[i].split(",");
					final String datasetTypeAndOptions = datasetInfo[2];
					final String datasetType = datasetTypeAndOptions.split(":")[0];
					final Map<String, String> datasetOptions = Stream
							.of(datasetTypeAndOptions.split(":"))
							.skip(1)
							.map(s -> s.split("="))
							.collect(Collectors.toMap(a -> a[0], a -> a[1]));

					switch (datasetType.toLowerCase()) {
						case RAW_IDENTIFIER:
							LOG.info(String.format("Handling dataset #%d as RAW data", i));
							handleRawDataset(sc, datasetInfo, blockSize, scales, downsamplingBlockSizes, clp.outputN5, clp.revert, resolution, offset);
							break;
						case LABEL_IDENTIFIER:
							LOG.info(String.format("Handling dataset #%d as LABEL data", i));
							handleLabelDataset(
									sc,
									datasetInfo,
									blockSize,
									scales,
									downsamplingBlockSizes,
									maxNumEntriesArray,
									clp.outputN5,
									clp.revert,
									clp.winnerTakesAll,
									labelBlockLookupN5BlockSize,
									resolution,
									offset);
							break;
						case CHANNEL_IDENTIFIER:
							LOG.info(String.format("Handling dataset #%d as CHANNEL data", i));
							handleChannelDataset(sc, datasetInfo, blockSize, scales, downsamplingBlockSizes, clp.outputN5, clp.revert, datasetOptions, resolution, offset);
							break;
						default:
							LOG.error(String.format("Did not recognize dataset type '%s' in dataset at position %d!", datasetInfo[2].toLowerCase(), i));
							break;
					}
				}
			}
		}
	}

	private static void convertAll(
			final String n5Container,
			final String targetGroup,
			final JavaSparkContext sc,
			final N5Reader n5Reader,
			final int[] blockSize,
			final int[][] downsamplingBlockSizes,
			final int[] maxNumEntriesArray,
			final int[][] scales,
			final String outputN5,
			final boolean revert,
			final boolean winnerTakesAll,
			final Optional<Integer> labelBlockLookupN5BlockSize,
			Optional< double[] > resolution,
			Optional< double[] > offset ) throws IOException, InvalidDataType, InvalidN5Container, InvalidDataset, InputSameAsOutput
	{
		final String[] subGroupNames = n5Reader.list( targetGroup );
		for ( final String subGroupName : subGroupNames )
		{
			final String fullSubGroupName = Paths.get( targetGroup, subGroupName ).toString();
			LOG.debug( String.format( "Checking subgroup %s ", fullSubGroupName ) );
			if ( n5Reader.datasetExists( fullSubGroupName ) )
			{
				try
				{
					if ( isLabelDataType( n5Reader, fullSubGroupName ) )
					{
						LOG.info( String.format( "Autodetected dataset %s as LABEL data", fullSubGroupName ) );
						handleLabelDataset(
								sc,
								new String[] { n5Container, fullSubGroupName, LABEL_IDENTIFIER },
								blockSize,
								scales,
								downsamplingBlockSizes,
								maxNumEntriesArray,
								outputN5,
								revert,
								winnerTakesAll,
								labelBlockLookupN5BlockSize,
								resolution,
								offset );
					}
					else
					{
						if (n5Reader.getDatasetAttributes(fullSubGroupName).getNumDimensions() > 3) {
							LOG.info(String.format("Autodetected dataset %s as CHANNEL data (more than three dimensions)", fullSubGroupName));
							handleChannelDataset(
									sc,
									new String[]{n5Container, fullSubGroupName, CHANNEL_IDENTIFIER},
									blockSize,
									scales,
									downsamplingBlockSizes,
									outputN5,
									revert,
									new HashMap<>(),
									resolution,
									offset);
						} else {
							LOG.info(String.format("Autodetected dataset %s as RAW data", fullSubGroupName));
							handleRawDataset(
									sc,
									new String[]{n5Container, fullSubGroupName, RAW_IDENTIFIER},
									blockSize,
									scales,
									downsamplingBlockSizes,
									outputN5,
									revert,
									resolution,
									offset);
						}
					}
				}
				catch ( final Exception e )
				{
					e.printStackTrace();
				}
			}
			else
			{
				convertAll(
						n5Container,
						fullSubGroupName,
						sc,
						n5Reader,
						blockSize,
						downsamplingBlockSizes,
						maxNumEntriesArray,
						scales,
						outputN5,
						revert,
						winnerTakesAll,
						labelBlockLookupN5BlockSize,
						resolution,
						offset );
			}
		}
	}

	static boolean isLabelDataType( final N5Reader n5Reader, final String fullSubGroupName ) throws IOException
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

	private static void handleRawDataset(
			final JavaSparkContext sc,
			final String[] datasetInfo,
			final int[] blockSize,
			final int[][] scales,
			final int[][] downsamplingBlockSizes,
			final String outputN5,
			final boolean revert,
			Optional< double[] > resolution,
			Optional< double[] > offset ) throws IOException {
		final String inputN5 = datasetInfo[0];
		final String inputDataset = datasetInfo[1];
		final String outputGroupName = (datasetInfo.length == 4) ? datasetInfo[3] : inputDataset;
		final String fullGroup = outputGroupName;

		final N5FSWriter writer = new N5FSWriter(outputN5, DEFAULT_BUILDER);
		writer.createGroup(fullGroup);

		setPainteraDataType(writer, fullGroup, RAW_IDENTIFIER);

		final String dataGroup = Paths.get(fullGroup, "data").toString();
		writer.createGroup(dataGroup);
		writer.setAttribute(dataGroup, "multiScale", true);

		final String outputDataset = Paths.get(dataGroup, "s0").toString();
		N5ConvertSpark.convert(sc,
				() -> N5Helpers.n5Reader(inputN5),
				inputDataset,
				() -> new N5FSWriter(outputN5, DEFAULT_BUILDER),
				outputDataset,
				Optional.of(blockSize),
				Optional.of(new GzipCompression()), // TODO pass compression
				// as parameter
				Optional.ofNullable(null),
				Optional.ofNullable(null),
				false);

		final double[] downsamplingFactor = DoubleStream.generate(() -> 1.0).limit(blockSize.length).toArray();

		for (int scaleNum = 0; scaleNum < scales.length; ++scaleNum) {
			final String newScaleDataset = Paths.get(dataGroup, String.format("s%d", scaleNum + 1)).toString();

			N5DownsamplerSpark.downsample(sc,
					() -> new N5FSWriter(outputN5, DEFAULT_BUILDER),
					Paths.get(dataGroup, String.format("s%d", scaleNum)).toString(),
					newScaleDataset,
					scales[scaleNum],
					downsamplingBlockSizes[scaleNum]);

			for (int i = 0; i < downsamplingFactor.length; ++i) {
				downsamplingFactor[i] *= scales[scaleNum][i];
			}
			writer.setAttribute(newScaleDataset, "downsamplingFactors", downsamplingFactor);

		}

		final double[] res = resolution.isPresent() ? resolution.get() : N5Helpers.revertInplaceAndReturn(
				tryGetDoubleArrayAttributeOrLongArrayAttributeAsDoubleArray(N5Helpers.n5Reader(inputN5), inputDataset, RESOLUTION_KEY),
				revert);
		if (res != null) {
			writer.setAttribute(Paths.get(fullGroup, "data").toString(), RESOLUTION_KEY, res);
		}

		final double[] off = offset.isPresent() ? offset.get() : N5Helpers.revertInplaceAndReturn(
				tryGetDoubleArrayAttributeOrLongArrayAttributeAsDoubleArray(N5Helpers.n5Reader(inputN5), inputDataset, OFFSET_KEY),
				revert );
		if (off != null) {
			writer.setAttribute(Paths.get(fullGroup, "data").toString(), OFFSET_KEY, off);
		}
	}

	private static int[] insertValueAt(final int[] array, final int value, final int position) {
		assert position >= 0;
		assert position <= array.length;
		final IntStream before = IntStream.of(array).limit(position);
		final IntStream after  = IntStream.of(array).skip(position);
		return IntStream.concat(IntStream.concat(before, IntStream.of(value)), after).toArray();
	}

	private static void handleChannelDataset(
			final JavaSparkContext sc,
			final String[] datasetInfo,
			final int[] blockSize,
			final int[][] scales,
			final int[][] downsamplingBlockSizes,
			final String outputN5,
			final boolean revert,
			final Map<String, String> datasetTypeParameters,
			Optional< double[] > resolution,
			Optional< double[] > offset ) throws IOException, IncompatibleChannelAxis {


		final String inputN5 = datasetInfo[0];
		final String inputDataset = datasetInfo[1];
		final String outputGroupName = (datasetInfo.length == 4) ? datasetInfo[3] : inputDataset;

		final DatasetAttributes attributes = N5Helpers.n5Reader(inputN5).getDatasetAttributes(inputDataset);

		final int channelAxis = datasetTypeParameters.containsKey(CHANNEL_AXIS_KEY)
				? Integer.parseInt(datasetTypeParameters.get(CHANNEL_AXIS_KEY))
				: attributes.getNumDimensions() - 1;
		if (channelAxis < 0 || channelAxis >= attributes.getNumDimensions())
			throw new IncompatibleChannelAxis(attributes.getNumDimensions(), channelAxis);

		final int channelBlockSize = datasetTypeParameters.containsKey(CHANNEL_BLOCKSIZE_KEY)
				? Integer.parseInt(datasetTypeParameters.get(CHANNEL_BLOCKSIZE_KEY))
				: 1;

		handleRawDataset(
				sc,
				datasetInfo,
				insertValueAt(blockSize, channelBlockSize,  channelAxis),
				Stream.of(scales).map(a -> insertValueAt(a, 1, channelAxis)).toArray(int[][]::new),
				Stream.of(downsamplingBlockSizes).map(a -> insertValueAt(a, channelBlockSize, channelAxis)).toArray(int[][]::new),
				outputN5,
				revert,
				resolution,
				offset
		);

		final N5FSWriter writer = new N5FSWriter(outputN5, DEFAULT_BUILDER);
		setPainteraDataType(writer, outputGroupName, CHANNEL_IDENTIFIER);
		writer.setAttribute(outputGroupName, CHANNEL_AXIS_KEY, channelAxis);

		final double[] downsamplingFactors = DoubleStream.generate(() -> 1.0).limit(blockSize.length).toArray();
		for (int i = 0, k = 1; i < scales.length; ++i, ++k) {
			for (int d = 0; d < downsamplingFactors.length; ++d) {
				downsamplingFactors[d] *= scales[i][d];
			}
			writer.setAttribute(Paths.get(outputGroupName, "data", "s" + k).toString(), "downsamplingFactors", downsamplingFactors);
		}

	}

	private static void handleLabelDataset(
			final JavaSparkContext sc,
			final String[] datasetInfo,
			final int[] initialBlockSize,
			final int[][] scales,
			final int[][] downsampleBlockSizes,
			final int[] maxNumEntriesArray,
			final String outputN5,
			final boolean revert,
			final boolean winnerTakesAll,
			Optional<Integer> labelBlockLookupN5BlockSize,
			Optional< double[] > resolution,
			Optional< double[] > offset ) throws IOException, InvalidDataType, InvalidN5Container, InvalidDataset, InputSameAsOutput
	{
		final String inputN5 = datasetInfo[ 0 ];
		final String inputDataset = datasetInfo[ 1 ];
		final String outputGroupName = ( datasetInfo.length == 4 ) ? datasetInfo[ 3 ] : inputDataset;
		final String fullGroup = outputGroupName;

		final N5FSWriter writer = new N5FSWriter( outputN5, DEFAULT_BUILDER );
		writer.createGroup( fullGroup );

		setPainteraDataType( writer, fullGroup, LABEL_IDENTIFIER );

		final String dataGroup = Paths.get( fullGroup, "data" ).toString();
		writer.createGroup( dataGroup );
		writer.setAttribute( dataGroup, "multiScale", true );
		final String outputDataset = Paths.get( fullGroup, "data", "s0" ).toString();
		final String uniqueLabelsGroup = Paths.get( fullGroup, "unique-labels" ).toString();
		final String labelBlockMappingGroup = Paths.get(fullGroup, "label-to-block-mapping" ).toString();
		final String labelBlockMappingGroupDirectory = Paths.get( outputN5, labelBlockMappingGroup ).toAbsolutePath().toString();

		if ( winnerTakesAll )
		{
			N5ConvertSpark.convert( sc,
					() -> N5Helpers.n5Reader( inputN5 ),
					inputDataset,
					() -> new N5FSWriter( outputN5, DEFAULT_BUILDER ),
					outputDataset,
					Optional.of( initialBlockSize ),
					Optional.of( new GzipCompression() ), // TODO pass
															// compression
															// as parameter
					Optional.ofNullable( null ),
					Optional.ofNullable( null ),
					false );

			final double[] downsamplingFactor = new double[] { 1.0, 1.0, 1.0 };

			for ( int scaleNum = 0; scaleNum < scales.length; ++scaleNum )
			{
				final String newScaleDataset = Paths.get( dataGroup, String.format( "s%d", scaleNum + 1 ) ).toString();

				N5LabelDownsamplerSpark.downsampleLabel( sc,
						() -> new N5FSWriter( outputN5, DEFAULT_BUILDER ),
						Paths.get( dataGroup, String.format( "s%d", scaleNum ) ).toString(),
						newScaleDataset,
						scales[ scaleNum ],
						downsampleBlockSizes[ scaleNum ] );

				for ( int i = 0; i < downsamplingFactor.length; ++i )
				{
					downsamplingFactor[ i ] *= scales[ scaleNum ][ i ];
				}
				writer.setAttribute( newScaleDataset, "downsamplingFactors", downsamplingFactor );

			}

			Long maxId = ExtractUniqueLabelsPerBlock.extractUniqueLabels( sc, outputN5, outputN5, outputDataset, Paths.get( uniqueLabelsGroup, "s0" ).toString() );
			LabelListDownsampler.addMultiScaleTag( writer, uniqueLabelsGroup );

			writer.setAttribute( fullGroup, "maxId", maxId );

			if ( scales.length > 0 ) // TODO refactor this to be nicer
			{
				LabelListDownsampler.donwsampleMultiscale( sc, outputN5, uniqueLabelsGroup, scales, downsampleBlockSizes );
			}
		}
		else
		{
			// TODO pass compression and reverse array as parameters
			ConvertToLabelMultisetType.convertToLabelMultisetType(
					sc,
					inputN5,
					inputDataset,
					initialBlockSize,
					outputN5,
					outputDataset,
					new GzipCompression(),
					revert );

			writer.setAttribute( fullGroup, "maxId", writer.getAttribute( outputDataset, "maxId", Long.class ) );

			ExtractUniqueLabelsPerBlock.extractUniqueLabels( sc, outputN5, outputN5, outputDataset, Paths.get( uniqueLabelsGroup, "s0" ).toString() );
			LabelListDownsampler.addMultiScaleTag( writer, uniqueLabelsGroup );

			if ( scales.length > 0 )
			{
				// TODO pass compression as parameter
				SparkDownsampler.downsampleMultiscale( sc, outputN5, dataGroup, scales, downsampleBlockSizes, maxNumEntriesArray, new GzipCompression() );
				LabelListDownsampler.donwsampleMultiscale( sc, outputN5, uniqueLabelsGroup, scales, downsampleBlockSizes );
			}
		}

		if ( labelBlockLookupN5BlockSize.isPresent() )
		{
			LabelToBlockMapping.createMappingWithMultiscaleCheckN5(sc, outputN5, uniqueLabelsGroup, outputN5, labelBlockMappingGroup, labelBlockLookupN5BlockSize.get());

		}
		else
		{
			LabelToBlockMapping.createMappingWithMultiscaleCheck(sc, outputN5, uniqueLabelsGroup, labelBlockMappingGroupDirectory);
		}
		if (writer.getAttributes(labelBlockMappingGroup).containsKey(LABEL_BLOCK_LOOKUP_KEY))
		{
			writer.setAttribute(fullGroup, LABEL_BLOCK_LOOKUP_KEY, writer.getAttribute(labelBlockMappingGroup, LABEL_BLOCK_LOOKUP_KEY, JsonElement.class));
		}

		final double[] res = resolution.isPresent() ? resolution.get() : N5Helpers.revertInplaceAndReturn(
				tryGetDoubleArrayAttributeOrLongArrayAttributeAsDoubleArray(N5Helpers.n5Reader(inputN5), inputDataset, RESOLUTION_KEY),
				revert );
		if ( res != null )
		{
			writer.setAttribute( Paths.get( fullGroup, "data" ).toString(), RESOLUTION_KEY, res );
		}

		final double[] off = offset.isPresent() ? offset.get() : N5Helpers.revertInplaceAndReturn(
				tryGetDoubleArrayAttributeOrLongArrayAttributeAsDoubleArray(N5Helpers.n5Reader(inputN5), inputDataset, OFFSET_KEY),
				revert );
		if ( off != null )
		{
			writer.setAttribute( Paths.get( fullGroup, "data" ).toString(), OFFSET_KEY, off );
		}
	}

	private static void setPainteraDataType( final N5FSWriter writer, final String group, final String type ) throws IOException
	{
		final HashMap< String, String > painteraDataType = new HashMap<>();
		painteraDataType.put( "type", type );
		writer.setAttribute( group, "painteraData", painteraDataType );
	}

	private static double[] tryGetDoubleArrayAttributeOrLongArrayAttributeAsDoubleArray(final N5Reader reader, final String dataset, final String attribute) throws IOException {
		try {
			return reader.getAttribute(dataset, attribute, double[].class);
		} catch (ClassCastException e) {
			return LongStream.of(reader.getAttribute(dataset, attribute, long[].class)).asDoubleStream().toArray();
		}
	}
}
