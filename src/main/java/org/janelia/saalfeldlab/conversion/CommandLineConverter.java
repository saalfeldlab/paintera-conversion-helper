package org.janelia.saalfeldlab.conversion;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.multisets.spark.convert.ConvertToLabelMultisetType;
import org.janelia.saalfeldlab.multisets.spark.downsample.SparkDownsampler;
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

		@Option( names = { "-r", "--outputN5" }, required = true )
		private String outputN5;

		@Option( names = { "-g", "--outputgroup" }, required = true )
		private String outputGroup;

		@Option( names = { "-b", "--blocksize" }, arity = "1", required = false )
		private String blockSize = "64,64,64";

		@Option( names = { "-h", "--help" }, usageHelp = true, description = "display a help message" )
		private boolean helpRequested;
	}

	public static void main( final String[] args ) throws IOException, CmdLineException
	{
		final CommandLineParameters clp = new CommandLineParameters();
		final CommandLine cl = new CommandLine( clp );
		cl.parse( args );

		if ( cl.isUsageHelpRequested() )
		{
			cl.usage( System.out );
			return;
		}

		String[] formattedScales = ( clp.scales != null ? new String[ clp.scales.length ] : null );
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

		String formattedBlockSize = clp.blockSize.split( "," ).length == 1 ? String.join( ",", clp.blockSize, clp.blockSize, clp.blockSize ) : clp.blockSize;

		for ( int i = 0; i < clp.datasets.length; ++i )
		{
			final String[] datasetInfo = clp.datasets[ i ].split( "," );
			switch ( datasetInfo[ 2 ].toLowerCase() )
			{
			case "raw":
				LOG.info( String.format( "Handling dataset #%d as RAW data", i ) );
				handleRawDataset( datasetInfo, formattedScales, clp.outputN5, clp.outputGroup, formattedBlockSize );
				break;
			case "label":
				LOG.info( String.format( "Handling dataset #%d as LABEL data", i ) );
				handleLabelDataset( datasetInfo, formattedScales, clp.outputN5, clp.outputGroup, formattedBlockSize );
				break;
			default:
				LOG.error( String.format( "Did not recognize dataset type '%s' in dataset at position %d!", datasetInfo[ 2 ].toLowerCase(), i ) );
				break;
			}
		}
	}

	private static void handleRawDataset( String[] datasetInfo, String[] scales, String outputN5, String outputGroup, String blockSize ) throws IOException, CmdLineException
	{
		final String inputN5 = datasetInfo[ 0 ];
		final String inputDataset = datasetInfo[ 1 ];
		final String outputGroupName = ( datasetInfo.length == 4 ) ? datasetInfo[ 3 ] : "raw";
		final String fullGroup = Paths.get( outputGroup, outputGroupName ).toString();

		N5FSWriter writer = new N5FSWriter( outputN5 );
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

			double[] downsamplingFactor = new double[] { 1.0, 1.0, 1.0 };

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
			writer.setAttribute( Paths.get( fullGroup, "data" ).toString(), "resolution", resolution );
	}

	private static void handleLabelDataset( String[] datasetInfo, String[] scales, String outputN5, String outputGroup, String blockSize ) throws IOException
	{
		final String inputN5 = datasetInfo[ 0 ];
		final String inputDataset = datasetInfo[ 1 ];
		final String outputGroupName = ( datasetInfo.length == 4 ) ? datasetInfo[ 3 ] : "label";
		final String fullGroup = Paths.get( outputGroup, outputGroupName ).toString();

		N5FSWriter writer = new N5FSWriter( outputN5 );
		writer.createGroup( fullGroup );

		setPainteraDataType( writer, fullGroup, "label" );

		if ( scales == null )
		{
			final String outputDataset = Paths.get( fullGroup, "data" ).toString();
			ConvertToLabelMultisetType.run( "--input-n5", inputN5, "--dataset", inputDataset,
					"--output-n5", outputN5, "--block-size", blockSize, outputDataset );
		}
		else
		{
			final String dataGroup = Paths.get( fullGroup, "data" ).toString();
			writer.createGroup( dataGroup );
			writer.setAttribute( dataGroup, "multiScale", true );
			final String outputDataset = Paths.get( dataGroup, "s0" ).toString();

			ConvertToLabelMultisetType.run( "--input-n5", inputN5, "--dataset", inputDataset,
					"--output-n5", outputN5, "--block-size", blockSize, outputDataset );

			SparkDownsampler.run( Stream.concat( Stream.of( "--n5-root", outputN5, "--group", dataGroup, "--block-size", blockSize ),
					Stream.of( scales ) ).toArray( String[]::new ) );

		}

		final JsonElement resolution = new N5FSReader( inputN5 ).getAttributes( inputDataset ).get( "resolution" );
		if ( resolution != null )
			writer.setAttribute( Paths.get( fullGroup, "data" ).toString(), "resolution", resolution );
	}

	private static void setPainteraDataType( N5FSWriter writer, String group, String type ) throws IOException
	{
		HashMap< String, String > painteraDataType = new HashMap< String, String >();
		painteraDataType.put( "type", type );
		writer.setAttribute( group, "painteraData", painteraDataType );
	}
}
