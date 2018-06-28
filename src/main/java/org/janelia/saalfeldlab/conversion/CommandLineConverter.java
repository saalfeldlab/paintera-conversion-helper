package org.janelia.saalfeldlab.conversion;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.multisets.spark.N5Helpers;
import org.janelia.saalfeldlab.multisets.spark.convert.ConvertToLabelMultisetType;
import org.janelia.saalfeldlab.multisets.spark.downsample.SparkDownsampler;
import org.janelia.saalfeldlab.multisets.spark.exception.InputSameAsOutput;
import org.janelia.saalfeldlab.multisets.spark.exception.InvalidDataType;
import org.janelia.saalfeldlab.multisets.spark.exception.InvalidDataset;
import org.janelia.saalfeldlab.multisets.spark.exception.InvalidN5Container;
import org.janelia.saalfeldlab.multisets.spark.uniquelabels.ExtractUniqueLabelsPerBlock;
import org.janelia.saalfeldlab.multisets.spark.uniquelabels.LabelToBlockMapping;
import org.janelia.saalfeldlab.multisets.spark.uniquelabels.downsample.LabelListDownsampler;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.spark.N5ConvertSpark;
import org.janelia.saalfeldlab.n5.spark.downsample.N5DownsamplerSpark;
import org.kohsuke.args4j.CmdLineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;

import picocli.CommandLine;
import picocli.CommandLine.Option;

public class CommandLineConverter
{
	private static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	public static class CommandLineParameters
	{
		@Option( names = { "-d", "--dataset" }, required = true,
				description = "Comma delimited description of dataset; <n5 root path>,<path/to/dataset>,<raw|label>[,optional name]" )
		private String[] datasets;

		@Option( names = { "-s", "--scale" }, arity = "1..*", description = "Factor by which to downscale the input image. Factors are relative to the previous level, not to level zero. Format either fx,fy,fz or f" )
		private String[] scales;

		@Option( names = { "-o", "--outputN5" }, required = true )
		private String outputN5;

		@Option( names = { "-b", "--blocksize" }, arity = "1", required = false )
		private String blockSize;

		@Option( names = { "-h", "--help" }, usageHelp = true, description = "display a help message" )
		private boolean helpRequested;
	}

	public static void main( final String[] args ) throws IOException, CmdLineException, InvalidDataType, InvalidN5Container, InvalidDataset, InputSameAsOutput
	{
		final CommandLineParameters clp = new CommandLineParameters();
		final CommandLine cl = new CommandLine( clp );
		cl.parse( args );

		if ( cl.isUsageHelpRequested() )
		{
			cl.usage( System.out );
			return;
		}

		clp.blockSize = clp.blockSize == null ? "64,64,64" : clp.blockSize;

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
		final int[][] scales = Arrays
				.stream( formattedScales )
				.map( fs -> fs.split( "," ) )
				.map( s -> Arrays.stream( s ).mapToInt( Integer::parseInt ).toArray() )
				.toArray( int[][]::new );

		final String formattedBlockSize = clp.blockSize.split( "," ).length == 1 ? String.join( ",", clp.blockSize, clp.blockSize, clp.blockSize ) : clp.blockSize;
		final int[] blockSize = Arrays
				.stream( clp.blockSize.split( "," ) )
				.mapToInt( Integer::parseInt )
				.toArray();
		final int[][] blockSizes = Stream.generate( () -> blockSize ).limit( scales.length ).toArray( int[][]::new );
		final int[] maxNumEntriesArray = IntStream.generate( () -> -1 ).limit( scales.length ).toArray();

		for ( int i = 0; i < clp.datasets.length; ++i )
		{
			final String[] datasetInfo = clp.datasets[ i ].split( "," );
			switch ( datasetInfo[ 2 ].toLowerCase() )
			{
			case "raw":
				LOG.info( String.format( "Handling dataset #%d as RAW data", i ) );
				handleRawDataset( datasetInfo, formattedScales, clp.outputN5, formattedBlockSize );
				break;
			case "label":
				LOG.info( String.format( "Handling dataset #%d as LABEL data", i ) );
				final SparkConf conf = new SparkConf().setAppName( MethodHandles.lookup().lookupClass().getName() + " " + Arrays.toString( args ) );
				try (JavaSparkContext sc = new JavaSparkContext( conf ))
				{
					handleLabelDataset( sc, datasetInfo, blockSize, scales, blockSizes, maxNumEntriesArray, clp.outputN5 );
				}
				break;
			default:
				LOG.error( String.format( "Did not recognize dataset type '%s' in dataset at position %d!", datasetInfo[ 2 ].toLowerCase(), i ) );
				break;
			}
		}
	}

	private static void handleRawDataset( final String[] datasetInfo, final String[] scales, final String outputN5, final String blockSize ) throws IOException, CmdLineException
	{
		final String inputN5 = datasetInfo[ 0 ];
		final String inputDataset = datasetInfo[ 1 ];
		final String outputGroupName = ( datasetInfo.length == 4 ) ? datasetInfo[ 3 ] : inputDataset;
		final String fullGroup = outputGroupName;

		final N5FSWriter writer = new N5FSWriter( outputN5 );
		writer.createGroup( fullGroup );

		setPainteraDataType( writer, fullGroup, "raw" );

		if ( scales == null )
		{
			final String outputDataset = Paths.get( fullGroup, "data" ).toString();
			N5ConvertSpark.main( "--inputN5Path", inputN5, "--inputDatasetPath", inputDataset,
					"--outputN5Path", outputN5, "--outputDatasetPath", outputDataset, "--blockSize", blockSize );
		}
		else
		{
			final String dataGroup = Paths.get( fullGroup, "data" ).toString();
			writer.createGroup( dataGroup );
			writer.setAttribute( dataGroup, "multiScale", true );
			final String outputDataset = Paths.get( dataGroup, "s0" ).toString();
			N5ConvertSpark.main( "--inputN5Path", inputN5, "--inputDatasetPath", inputDataset,
					"--outputN5Path", outputN5, "--outputDatasetPath", outputDataset, "--blockSize", blockSize );

			final double[] downsamplingFactor = new double[] { 1.0, 1.0, 1.0 };

			for ( int scaleNum = 0; scaleNum < scales.length; ++scaleNum )
			{
				final String newScaleDataset = Paths.get( dataGroup, String.format( "s%d", scaleNum + 1 ) ).toString();
				N5DownsamplerSpark.main( "--n5Path", outputN5, "--inputDatasetPath", Paths.get( dataGroup, String.format( "s%d", scaleNum ) ).toString(),
						"--outputDatasetPath", newScaleDataset, "--factors", scales[ scaleNum ], "--blockSize", blockSize );

				final String[] thisScale = scales[ scaleNum ].split( "," );
				for ( int i = 0; i < downsamplingFactor.length; ++i )
				{
					downsamplingFactor[ i ] *= Double.parseDouble( thisScale[ i ] );
				}
				writer.setAttribute( newScaleDataset, "downsamplingFactors", downsamplingFactor );
			}
		}

		final JsonElement resolution = new N5FSReader( inputN5 ).getAttributes( inputDataset ).get( "resolution" );
		if ( resolution != null )
		{
			writer.setAttribute( Paths.get( fullGroup, "data" ).toString(), "resolution", resolution );
		}
	}

	private static void handleLabelDataset(
			final JavaSparkContext sc,
			final String[] datasetInfo,
			final int[] initialBlockSize,
			final int[][] scales,
			final int[][] blockSizes,
			final int[] maxNumEntriesArray,
			final String outputN5 ) throws IOException, InvalidDataType, InvalidN5Container, InvalidDataset, InputSameAsOutput
	{
		final String inputN5 = datasetInfo[ 0 ];
		final String inputDataset = datasetInfo[ 1 ];
		final String outputGroupName = ( datasetInfo.length == 4 ) ? datasetInfo[ 3 ] : inputDataset;
		final String fullGroup = outputGroupName;

		final N5FSWriter writer = new N5FSWriter( outputN5 );
		writer.createGroup( fullGroup );

		setPainteraDataType( writer, fullGroup, "label" );

		final String dataGroup = Paths.get( fullGroup, "data" ).toString();
		writer.createGroup( dataGroup );
		writer.setAttribute( dataGroup, "multiScale", true );
		final String outputDataset = Paths.get( fullGroup, "data", "s0" ).toString();
		final String uniqueLabelsGroup = Paths.get( fullGroup, "unique-labels" ).toString();
		final String labelBlockMappingGroupDirectory = Paths.get( outputN5, fullGroup, "label-to-block-mapping" ).toAbsolutePath().toString();

		// TODO pass compression and reverse array as parameters
		ConvertToLabelMultisetType.convertToLabelMultisetType(
				sc,
				inputN5,
				inputDataset,
				initialBlockSize,
				outputN5,
				outputDataset,
				new GzipCompression(),
				false );

		writer.setAttribute( fullGroup, "maxId", writer.getAttribute( outputDataset, "maxId", Long.class ) );

		ExtractUniqueLabelsPerBlock.extractUniqueLabels( sc, outputN5, outputN5, outputDataset, Paths.get( uniqueLabelsGroup, "s0" ).toString() );
		LabelListDownsampler.addMultiScaleTag( writer, uniqueLabelsGroup );

		if ( scales.length > 0 )
		{
			// TODO pass compression as parameter
			SparkDownsampler.downsampleMultiscale( sc, outputN5, dataGroup, scales, blockSizes, maxNumEntriesArray, new GzipCompression() );
			LabelListDownsampler.donwsampleMultiscale( sc, outputN5, uniqueLabelsGroup, scales, blockSizes );
		}

		LabelToBlockMapping.createMappingWithMultiscaleCheck( sc, outputN5, uniqueLabelsGroup, labelBlockMappingGroupDirectory );

		final double[] resolution = N5Helpers.n5Reader( inputN5 ).getAttribute( inputDataset, "resolution", double[].class );
		if ( resolution != null )
		{
			writer.setAttribute( Paths.get( fullGroup, "data" ).toString(), "resolution", resolution );
		}

		final double[] offset = N5Helpers.n5Reader( inputN5 ).getAttribute( inputDataset, "offset", double[].class );
		if ( offset != null )
		{
			writer.setAttribute( Paths.get( fullGroup, "data" ).toString(), "offset", offset );
		}
	}

	private static void setPainteraDataType( final N5FSWriter writer, final String group, final String type ) throws IOException
	{
		final HashMap< String, String > painteraDataType = new HashMap<>();
		painteraDataType.put( "type", type );
		writer.setAttribute( group, "painteraData", painteraDataType );
	}
}
