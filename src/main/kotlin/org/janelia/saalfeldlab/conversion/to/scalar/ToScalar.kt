package org.janelia.saalfeldlab.paintera.conversion.to.scalar

import gnu.trove.map.TLongLongMap
import gnu.trove.map.hash.TLongLongHashMap
import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.conversion.*
import org.janelia.saalfeldlab.conversion.to.newSparkConf
import org.janelia.saalfeldlab.conversion.to.paintera.SpatialDoubleArray
import org.janelia.saalfeldlab.conversion.to.paintera.SpatialIntArray
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
		names = ["--scale"],
		arity = "1..*",
		split = "\\s",
		converter = [SpatialIntArray.Converter::class],
		paramLabel = SpatialIntArray.PARAM_LABEL,
		description = [
			"Relative downsampling factors for each level in the format x,y,z, where x,y,z are integers. Single integers u are interpreted as u,u,u.",
		]
	)
	private var _scales: Array<SpatialIntArray>? = null

	@CommandLine.Option(
		names = ["--downsample-block-sizes"],
		arity = "1..*",
		split = "\\s",
		converter = [SpatialIntArray.Converter::class],
		paramLabel = SpatialIntArray.PARAM_LABEL,
		description = ["Output block size per downsampled level; defaults to --block-size for every level."]
	)
	private var _downsampleBlockSizes: Array<SpatialIntArray>? = null

	@CommandLine.Option(
		names = ["--xyz-unit"],
		required = true,
		split = ",",
		description = ["Unit for the x, y, z (1 value, or 1 value per axis)."],
	)
	private lateinit var xyzUnit: Array<String>

	@CommandLine.Option(
		names = ["--resolution"],
		required = false,
		converter = [SpatialDoubleArray.Converter::class],
		paramLabel = SpatialDoubleArray.PARAM_LABEL,
		description = ["Physical resolution x,y,z (a single value u means u,u,u). Overrides the input's resolution attribute; needed for Paintera inputs, which store it on the data group rather than s0."]
	)
	private var _resolution: SpatialDoubleArray? = null

	@CommandLine.Option(
		names = ["--offset"],
		required = false,
		converter = [SpatialDoubleArray.Converter::class],
		paramLabel = SpatialDoubleArray.PARAM_LABEL,
		description = ["Physical offset x,y,z (a single value u means u,u,u). Overrides the input's offset attribute."]
	)
	private var _offset: SpatialDoubleArray? = null

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

			val xyzUnit = this.xyzUnit.let { units ->
				when (units.size) {
					1 -> Array(3) { units[0] }
					3 -> units.clone()
					else -> throw InvalidAxisUnit(units, "xyz-unit has to be specified with one or three entries but got ${units.joinToString(", ", "[", "]")}")
				}
			}

			val scales = _scales?.map { it.array }?.toTypedArray() ?: emptyArray()
			val downsampleBlockSizes = _downsampleBlockSizes?.map { it.array }?.toTypedArray()
				?.also { if (it.size != scales.size) throw InvalidBlockSize(blockSize, "--downsample-block-sizes must have one entry per --scale level (${scales.size}), but got ${it.size}") }
				?: Array(scales.size) { blockSize }

			extract(
				inputContainer,
				outputFormat,
				outputContainer,
				inputDataset,
				outputDataset,
				blockSize,
				chunksPerShard,
				xyzUnit,
				scales,
				downsampleBlockSizes,
				_resolution?.array,
				_offset?.array,
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
			xyzUnit: Array<String>,
			scales: Array<IntArray>,
			downsampleBlockSizes: Array<IntArray>,
			resolution: DoubleArray?,
			offset: DoubleArray?,
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
					xyzUnit,
					chunksPerShard,
					scales,
					downsampleBlockSizes,
					resolution,
					offset
				)
			}

			return 0

		}
	}
}