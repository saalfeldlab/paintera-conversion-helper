package org.janelia.saalfeldlab.paintera.conversion.to.scalar

import gnu.trove.map.TLongLongMap
import gnu.trove.map.hash.TLongLongHashMap
import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.conversion.*
import org.janelia.saalfeldlab.conversion.to.newSparkConf
import org.janelia.saalfeldlab.n5.universe.StorageFormat
import picocli.CommandLine
import java.io.IOException
import java.net.URI
import java.util.concurrent.Callable

@CommandLine.Command(
	name = "to-scalar",
	exitCodeOnSuccess = PainteraConvert.EXIT_CODE_SUCCESS,
	exitCodeOnInvalidInput = PainteraConvert.EXIT_CODE_INVALID_INPUT,
	exitCodeOnUsageHelp = PainteraConvert.EXIT_CODE_HELP_REQUESTED,
	exitCodeOnVersionHelp = PainteraConvert.EXIT_CODE_HELP_REQUESTED,
	exitCodeOnExecutionException = PainteraConvert.EXIT_CODE_EXECUTION_EXCEPTION,
	aliases = ["ts"],
	description = ["" +
			"Convert non-scalar label data to UINT64 scalar dataset.  " +
			"The input can be a single-scale dataset, a multi-scale group, or a Paintera dataset."]
)
class ToScalar : Callable<Int> {

	@CommandLine.Option(names = ["--input-container", "-i"], required = true)
	private lateinit var inputContainer: String

	@CommandLine.Option(
		names = ["--input-dataset", "-I"], required = true, description = ["" +
				"Can be a Paintera dataset, multi-scale N5 group, or regular dataset. " +
				"Highest resolution dataset will be used for Paintera dataset (data/s0) and multi-scale group (s0)."]
	)
	private lateinit var inputDataset: String

	@CommandLine.Option(names = ["--output-container", "-o"], required = true)
	private lateinit var _outputContainer: String

	@CommandLine.Option(names = ["--output-format"], required = false, defaultValue = "", paramLabel = "OUTPUT_FORMAT")
	private var _outputFormat: String = ""

	/* explicit --output-format, else the storage scheme of the container, else inferred on write */
	private val outputFormat: StorageFormat?
		get() = runCatching { StorageFormat.valueOf(_outputFormat) }.getOrNull()
			?: StorageFormat.parseUri(_outputContainer).a

	private val outputContainer: URI
		get() = StorageFormat.parseUri(_outputContainer).b

	@CommandLine.Option(names = ["--output-dataset", "-O"], required = false, description = ["defaults to input dataset"])
	internal var outputDataset: String? = null

	@CommandLine.Option(
		names = ["--block-size"],
		required = false,
		split = ",",
		description = ["Block size for output dataset. Will default to block size of input dataset if not specified."],
		defaultValue = "64,64,64"
	)
	private lateinit var blockSize: IntArray

	@CommandLine.Option(
		names = ["--chunks-per-shard"],
		required = false,
		split = ",",
		description = ["Number of chunks per shard, per axis (one value or three). Only valid for OUTPUT_FORMAT=ZARR3"]
	)
	private var chunksPerShard: IntArray? = null

	@CommandLine.Option(
		names = ["--consider-fragment-segment-assignment"],
		required = false,
		defaultValue = "false",
		description = ["Consider fragment-segment-assignment inside Paintera dataset. Will be ignored if not a Paintera dataset"]
	)
	internal var considerFragmentSegmentAssignment: Boolean = false

	@CommandLine.Option(
		names = ["--spark-master"],
		required = false,
		description = ["Spark master URL. Default will run locally with up to 24 workers (e.g. local[24] )."]
	)
	var sparkMaster: String? = null

	@CommandLine.Option(
		names = ["--additional-assignment"],
		split = ",",
		required = false,
		converter = [ExtractHighestResolutionLabelDataset.LookupPair.Converter::class],
		paramLabel = "from=to",
		description = ["Add additional lookup-values in the format `from=to'. Warning: Consistency with fragment-segment-assignment is not enforced."]
	)
	internal var additionalAssignments: Array<ExtractHighestResolutionLabelDataset.LookupPair>? = null

	@CommandLine.Option(names = ["--help"], help = true, usageHelp = true)
	var helpRequested: Boolean = false

	@Throws(IOException::class, ConversionException::class)
	override fun call(): Int {

		if (helpRequested)
			return 0

		val outputDataset = outputDataset ?: inputDataset

		return try {
			val inputUri = StorageFormat.parseUri(inputContainer).b
			if (inputUri == outputContainer && inputDataset == outputDataset)
				throw InvalidOutputDataset(
					outputContainer.toString(),
					outputDataset,
					"Input and output are the same datasets `$outputDataset' in the same container `$outputContainer'"
				)

			val assignment = TLongLongHashMap().also { m -> additionalAssignments?.forEach { m.put(it.key, it.value) } }

			val blockSize = this.blockSize.let { bs ->
				when (bs.size) {
					1 -> IntArray(3) { bs[0] }
					3 -> bs.clone()
					else -> throw InvalidBlockSize(bs, "Block size has to be specified with one or three entries but got ${bs.joinToString(", ", "[", "]")}")
				}
			}

			val chunksPerShard = this.chunksPerShard?.let { cps ->
				when (cps.size) {
					1 -> IntArray(3) { cps[0] }
					3 -> cps.clone()
					else -> throw InvalidBlockSize(cps, "chunks-per-shard has to be specified with one or three entries but got ${cps.joinToString(", ", "[", "]")}")
				}
			}

			extract(
				inputContainer,
				outputFormat,
				outputContainer,
				inputDataset,
				outputDataset,
				blockSize,
				chunksPerShard,
				considerFragmentSegmentAssignment,
				assignment,
				sparkMaster
			)
		} catch (conversionError: ConversionException) {
			System.err.println(conversionError.message)
			conversionError.exitCode
		} catch (error: Exception) {
			System.err.println("Unable to extract scalar labels: ${error.message}")
			PainteraConvert.EXIT_CODE_EXECUTION_EXCEPTION
		}
	}

	companion object {
		@Throws(IOException::class)
		private fun extract(
			inputContainer: String,
			outputFormat: StorageFormat?,
			outputContainer: URI,
			inputDataset: String,
			outputDataset: String,
			blockSize: IntArray,
			chunksPerShard: IntArray?,
			considerFragmentSegmentAssignment: Boolean,
			assignment: TLongLongMap,
			sparkMaster: String?
		): Int {

			val conf = newSparkConf(sparkMaster)

			JavaSparkContext(conf).use { sc ->
				ExtractHighestResolutionLabelDataset.extractNoGenerics(
					sc,
					{ createReader(inputContainer) },
					{ createWriter(outputFormat, outputContainer) },
					inputDataset,
					outputDataset,
					blockSize,
					considerFragmentSegmentAssignment,
					assignment,
					chunksPerShard
				)
			}

			return 0

		}
	}
}