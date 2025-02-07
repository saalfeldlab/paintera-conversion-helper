package org.janelia.saalfeldlab.conversion.to.paintera

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.ivy.core.IvyPatternHelper.TYPE_KEY
import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.conversion.ConversionException
import org.janelia.saalfeldlab.conversion.DatasetInfo
import org.janelia.saalfeldlab.conversion.PainteraConvert.Companion.EXIT_CODE_EXECUTION_EXCEPTION
import org.janelia.saalfeldlab.conversion.to.newSparkConf
import org.janelia.saalfeldlab.n5.N5Reader
import org.janelia.saalfeldlab.n5.N5Writer
import picocli.CommandLine
import java.io.File
import java.io.IOException
import java.net.URI
import java.util.concurrent.Callable
import kotlin.system.exitProcess

class ToPainteraData {

	companion object {

		private val LOG = KotlinLogging.logger { }

		@JvmStatic
		fun main(argv: Array<String>) {
			val args = Parameters()
			val cl = CommandLine(args)
			val exitCode = cl.execute(*argv)

			if (exitCode != 0)
				exitProcess(255 - exitCode)

			call(args)
		}

		fun call(args: Parameters) = args.call()
	}

	@CommandLine.Command(
		name = "to-paintera",
		sortOptions = false,
		aliases = ["tp"],
		usageHelpWidth = 120,
		header = [
			"Converts arbitrary 3D label and single- or multi-channel raw datasets in N5, Zarr, or HDF5 containers into a Paintera-friendly format."],
		footer = [
			"",
			"Example command for sample A of the CREMI challenge (https://cremi.org/static/data/sample_A_20160501.hdf):",
			"""
paintera-convert to-paintera \
  --scale 2,2,1 2,2,1 2,2,1 2 2 \
  --reverse-array-attributes \
  --output-container=paintera-converted.n5 \
  --container=sample_A_20160501.hdf \
    -d volumes/raw \
      --target-dataset=volumes/raw2 \
      --dataset-scale 3,3,1 3,3,1 2 2 \
      --dataset-resolution 4,4,40.0 \
    -d volumes/labels/neuron_ids
""",
		]
	)
	class Parameters : Callable<Int> {
		@CommandLine.ArgGroup(exclusive = false, multiplicity = "0..1")
		var parameters: GlobalParameters = GlobalParameters()

		@CommandLine.Option(names = ["--overwrite-existing"], defaultValue = "false")
		var overwriteExisting: Boolean = false

		@CommandLine.Option(names = ["--output-container"], required = true, paramLabel = "OUTPUT_CONTAINER")
		lateinit var _outputContainer: String

		@CommandLine.Option(
			names = ["--spark-master"],
			required = false,
			description = ["" + "Spark master URL. Defaults to local[X] where X is the number of cores, up to 24"]
		)
		var sparkMaster: String? = null

		@CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*")
		lateinit var containers: Array<ContainerParameters>

		@CommandLine.Option(names = ["--help"], help = true, usageHelp = true)
		var helpRequested: Boolean = false

		val outputContainer: String
			get() = File(_outputContainer).absolutePath

		override fun call(): Int {

			if (helpRequested)
				return 0

			parameters.call()
			containers.forEach { it.parameters.initGlobalParameters(parameters); it.call() }

			val datasets: MutableMap<DatasetInfo, Pair<DatasetConverter, DatasetSpecificParameters>> = mutableMapOf()
			val exceptions = mutableListOf<ConversionException>()
			for (container in containers) {
				container.parameters.initGlobalParameters(parameters)
				container.call()
				for (dataset in container.datasets) {
					val info = DatasetInfo(
						inputContainer = container.container.toString(),
						inputDataset = dataset.dataset,
						outputContainer = outputContainer,
						outputGroup = dataset.targetDataset
					)
					try {
						info.ensureInput()
						info.ensureOutput(overwriteExisting)
						if (info in datasets)
							throw ConversionException("Dataset specified multiple times: `$info'")
						val converter = DatasetConverter[info, dataset.parameters.type ?: info.type]
							?: throw ConversionException("Do not know how to convert dataset of type `${dataset.parameters.type}': `$info'")
						datasets[info] = Pair(converter, dataset.parameters)
					} catch (e: ConversionException) {
						exceptions += e
					}
				}
			}

			if (exceptions.isNotEmpty()) {
				LOG.error(exceptions[0]) { "Invalid options" }
				exceptions.forEach { LOG.error { it.message } }
				return exceptions[0].exitCode
			}

			var exitCode = runCatching {
				JavaSparkContext(newSparkConf(sparkMaster)).use { sc ->
					datasets.forEach { dataset, (converter, parameters) ->
						println("Converting dataset `$dataset'")
						converter.convert(sc, parameters, overwriteExisting)
					}
					0
				}
			}.getOrElse { cause ->
				LOG.error(cause) { "Unable to convert into Paintera dataset" }
				(cause as? ConversionException)?.exitCode ?: EXIT_CODE_EXECUTION_EXCEPTION
			}
			return exitCode
		}
	}

}

class GlobalParameters : Callable<Unit> {
	// TODO use custom class instead of IntArray
	@CommandLine.Option(
		names = ["--block-size"],
		description = ["Use --container-block-size and --dataset-block-size for container and dataset specific block sizes, respectively."],
		paramLabel = SpatialIntArray.PARAM_LABEL,
		defaultValue = "64,64,64",
		converter = [SpatialIntArray.Converter::class]
	)
	private lateinit var _blockSize: SpatialIntArray

	@CommandLine.Option(
		names = ["--scale"],
		arity = "1..*",
		description = [
			"Relative downsampling factors for each level in the format x,y,z, where x,y,z are integers. Single integers u are interpreted as u,u,u.",
			"Use --container-scale and --dataset-scale for container and dataset specific scales, respectively."],
		split = "\\s",
		converter = [SpatialIntArray.Converter::class],
		paramLabel = SpatialIntArray.PARAM_LABEL
	)
	private lateinit var _scales: Array<SpatialIntArray>

	@CommandLine.Option(
		names = ["--downsample-block-sizes"],
		description = ["Use --container-downsample-block-sizes and --dataset-downsample-block-sizes for container and dataset specific block sizes, respectively."],
		arity = "1..*",
		split = "\\s",
		converter = [SpatialIntArray.Converter::class],
		paramLabel = SpatialIntArray.PARAM_LABEL
	)
	private lateinit var _downsamplingBlockSizes: Array<SpatialIntArray>

	@CommandLine.Option(
		names = ["--reverse-array-attributes"],
		description = [
			"Reverse array attributes like resolution and offset, i.e. [x, y, z] -> [z, y, x].",
			"Use --container-reverse-array-attributes and --dataset-reverse-array-attributes for container and dataset specific setting, respectively."],
		defaultValue = "false"
	)
	var reverseArrayAttributes: Boolean = false

	@CommandLine.Option(
		names = ["--resolution"],
		description = [
			"Specify resolution (overrides attributes of input datasets, if any).",
			"Use --container-resolution and --dataset-resolution for container and dataset specific resolution, respectively."],
		converter = [SpatialDoubleArray.Converter::class],
		paramLabel = SpatialDoubleArray.PARAM_LABEL
	)
	var resolution: SpatialDoubleArray? = null

	@CommandLine.Option(
		names = ["--offset"],
		description = [
			"Specify offset (overrides attributes of input datasets, if any).",
			"Use --container-offset and --dataset-offset for container and dataset specific resolution, respectively."],
		converter = [SpatialDoubleArray.Converter::class],
		paramLabel = SpatialDoubleArray.PARAM_LABEL
	)
	var offset: SpatialDoubleArray? = null

	@CommandLine.Option(
		names = ["--max-num-entries", "-m"],
		description = [
			"" +
					"Limit number of entries for non-scalar label types by N. If N is negative, do not limit number of entries.  " +
					"If fewer values than the number of down-sampling layers are provided, the missing values are copied from the " +
					"last available entry.  If none are provided, default to -1 for all levels.",
			"Use --container-max-num-entries and --dataset-max-num-entries for container and dataset specific settings, respectively."],
		arity = "1..*",
		paramLabel = "N"
	)
	private var _maxNumEntries: IntArray? = null

	@CommandLine.Option(
		names = ["--label-block-lookup-n5-block-size"],
		description = [
			"Set the block size for the N5 container for the label-block-lookup.",
			"Use --container-label-block-lookup-n5-block-size and --dataset-label-block-lookup-n5-block-size for container and dataset specific settings, respectively."],
		defaultValue = "10000",
		paramLabel = "N"
	)
	var labelBlockLookupN5BlockSize: Int = 10000
		private set

	@CommandLine.Option(
		names = ["--winner-takes-all-downsampling"],
		description = [
			"Use scalar label type with winner-takes-all downsampling.",
			"Use --container-winner-takes-all-downsampling and --dataset-winner-takes-all-downsampling for container and dataset specific settings, respectively."],
		defaultValue = "false"
	)
	var winnerTakesAllDownsampling: Boolean = false
		private set

	val blockSize: SpatialIntArray
		get() = _blockSize

	val scales: Array<SpatialIntArray>
		get() = _scales

	val numScales: Int
		get() = scales.size

	val downsamplingBlockSizes
		get() = fillUpTo(if (_downsamplingBlockSizes.isEmpty()) arrayOf(blockSize) else _downsamplingBlockSizes, numScales)

	val maxNumEntries: IntArray
		get() = _maxNumEntries?.let { fillUpTo(if (it.isEmpty()) intArrayOf(-1) else it, numScales) } ?: IntArray(numScales) { -1 }

	override fun call() {
		ensureInitialized()
	}

	private fun ensureInitialized() {
		if (!this::_scales.isInitialized) _scales = arrayOf()
		if (!this::_downsamplingBlockSizes.isInitialized) _downsamplingBlockSizes = arrayOf()
	}

}

class ContainerParameters : Callable<Unit> {

	@CommandLine.Option(names = ["--container"], paramLabel = "CONTAINER")
	private lateinit var _container: URI

	@CommandLine.ArgGroup(exclusive = false)
	var parameters: ContainerSpecificParameters = ContainerSpecificParameters()
		private set

	@CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*")
	private lateinit var _datasets: Array<DatasetParameters>

	val container: URI
		get() = _container

	val datasets: Array<DatasetParameters>
		get() = _datasets

	override fun call() = datasets.forEach { it.parameters.initGlobalParameters(parameters) }

}

class DatasetParameters {

	@CommandLine.Option(names = ["--dataset", "-d"], required = true, paramLabel = "DATASET")
	private lateinit var _dataset: String

	@CommandLine.Option(names = ["--target-dataset"], paramLabel = "TARGET_DATASET")
	var _targetDataset: String? = null
		private set

	val dataset: String
		get() = _dataset

	val targetDataset: String
		get() = _targetDataset ?: dataset

	@CommandLine.ArgGroup(exclusive = false)
	var parameters: DatasetSpecificParameters = DatasetSpecificParameters()

}


// TODO introduce dataset-specific parameters
class ContainerSpecificParameters {

	private lateinit var globalParameters: GlobalParameters

	fun initGlobalParameters(globalParameters: GlobalParameters) {
		this.globalParameters = globalParameters
	}

	@CommandLine.Option(
		names = ["--container-block-size"],
		hidden = true,
		paramLabel = SpatialIntArray.PARAM_LABEL,
		defaultValue = "64,64,64",
		converter = [SpatialIntArray.Converter::class]
	)
	private var _blockSize: SpatialIntArray? = null

	@CommandLine.Option(
		names = ["--container-scale"],
		hidden = true,
		arity = "1..*",
		description = ["Relative downsampling factors for each level in the format x,y,z, where x,y,z are integers. Single integers u are interpreted as u,u,u"],
		split = "\\s",
		converter = [SpatialIntArray.Converter::class],
		paramLabel = SpatialIntArray.PARAM_LABEL
	)
	private var _scales: Array<SpatialIntArray>? = null

	@CommandLine.Option(
		names = ["--container-downsample-block-sizes"],
		hidden = true,
		arity = "1..*",
		split = "\\s",
		converter = [SpatialIntArray.Converter::class],
		paramLabel = SpatialIntArray.PARAM_LABEL
	)
	private var _downsamplingBlockSizes: Array<SpatialIntArray>? = null

	@CommandLine.Option(names = ["--container-reverse-array-attributes"], hidden = true)
	private var _reverseArrayAttributes: Boolean? = null

	@CommandLine.Option(
		names = ["--container-resolution"],
		hidden = true,
		converter = [SpatialDoubleArray.Converter::class],
		paramLabel = SpatialDoubleArray.PARAM_LABEL
	)
	private var _resolution: SpatialDoubleArray? = null

	@CommandLine.Option(
		names = ["--container-offset"],
		hidden = true,
		converter = [SpatialDoubleArray.Converter::class],
		paramLabel = SpatialDoubleArray.PARAM_LABEL
	)
	private var _offset: SpatialDoubleArray? = null

	@CommandLine.Option(names = ["--container-max-num-entries"], hidden = true, arity = "1..*", paramLabel = "N")
	private var _maxNumEntries: IntArray? = null

	@CommandLine.Option(names = ["--container-label-block-lookup-n5-block-size"], hidden = true, paramLabel = "N")
	private var _labelBlockLookupN5BlockSize: Int? = null

	@CommandLine.Option(names = ["--container-winner-takes-all-downsampling"], hidden = true)
	private var _winnerTakesAllDownsampling: Boolean? = null

	val blockSize: SpatialIntArray
		get() = _blockSize ?: globalParameters.blockSize

	val scales: Array<SpatialIntArray>
		get() = _scales ?: globalParameters.scales

	val numScales: Int
		get() = scales.size

	val downsamplingBlockSizes: Array<SpatialIntArray>
		get() = fillUpTo(
			(_downsamplingBlockSizes
				?: globalParameters.downsamplingBlockSizes).takeUnless { it.isEmpty() }
				?: arrayOf(blockSize), numScales)

	val reverseArrayAttributes: Boolean
		get() = _reverseArrayAttributes ?: globalParameters.reverseArrayAttributes

	val resolution: SpatialDoubleArray?
		get() = _resolution ?: globalParameters.resolution

	val offset: SpatialDoubleArray?
		get() = _offset ?: globalParameters.offset

	val maxNumEntries: IntArray
		get() = _maxNumEntries?.let { fillUpTo(if (it.isEmpty()) intArrayOf(-1) else it, numScales) } ?: fillUpTo(globalParameters.maxNumEntries, numScales)

	val labelBlockLookupN5BlockSize: Int
		get() = _labelBlockLookupN5BlockSize ?: globalParameters.labelBlockLookupN5BlockSize

	val winnerTakesAllDownsampling: Boolean
		get() = _winnerTakesAllDownsampling ?: globalParameters.winnerTakesAllDownsampling

//    @CommandLine.Option(names = ["--overwrite-existing"])
//    var overwriteExisiting: Boolean? = null

}

class DatasetSpecificParameters {

	private lateinit var containerParameters: ContainerSpecificParameters

	fun initGlobalParameters(containerParameters: ContainerSpecificParameters) {
		this.containerParameters = containerParameters
	}

	@CommandLine.Option(
		names = ["--dataset-block-size"],
		hidden = true,
		paramLabel = SpatialIntArray.PARAM_LABEL,
		converter = [SpatialIntArray.Converter::class]
	)
	private var _blockSize: SpatialIntArray? = null

	@CommandLine.Option(
		names = ["--dataset-scale"],
		hidden = true,
		arity = "1..*",
		description = ["Relative downsampling factors for each level in the format x,y,z, where x,y,z are integers. Single integers u are interpreted as u,u,u"],
		split = "\\s",
		converter = [SpatialIntArray.Converter::class],
		paramLabel = SpatialIntArray.PARAM_LABEL
	)
	private var _scales: Array<SpatialIntArray>? = null

	@CommandLine.Option(
		names = ["--dataset-downsample-block-sizes"],
		hidden = true,
		arity = "1..*",
		description = ["Relative downsampling factors for each level in the format x,y,z, where x,y,z are integers. Single integers u are interpreted as u,u,u"],
		split = "\\s",
		converter = [SpatialIntArray.Converter::class],
		paramLabel = SpatialIntArray.PARAM_LABEL
	)
	private var _downsamplingBlockSizes: Array<SpatialIntArray>? = null

	@CommandLine.Option(names = ["--dataset-reverse-array-attributes"], hidden = true)
	private var _reverseArrayAttributes: Boolean? = null

	@CommandLine.Option(
		names = ["--dataset-resolution"],
		hidden = true,
		converter = [SpatialDoubleArray.Converter::class],
		paramLabel = SpatialDoubleArray.PARAM_LABEL
	)
	private var _resolution: SpatialDoubleArray? = null

	@CommandLine.Option(
		names = ["--dataset-offset"],
		hidden = true,
		converter = [SpatialDoubleArray.Converter::class],
		paramLabel = SpatialDoubleArray.PARAM_LABEL
	)
	private var _offset: SpatialDoubleArray? = null

	@CommandLine.Option(names = ["--type"], completionCandidates = TypeOptions::class, required = false, paramLabel = "TYPE")
	private var _type: String? = null

	@CommandLine.Option(names = ["--dataset-max-num-entries"], hidden = true, arity = "1..*", paramLabel = "N")
	private var _maxNumEntries: IntArray? = null

	@CommandLine.Option(names = ["--dataset-label-block-lookup-n5-block-size"], hidden = true, paramLabel = "N")
	private var _labelBlockLookupN5BlockSize: Int? = null

	@CommandLine.Option(names = ["--dataset-winner-takes-all-downsampling"], hidden = true)
	private var _winnerTakesAllDownsampling: Boolean? = null

	val blockSize: SpatialIntArray
		get() = _blockSize ?: containerParameters.blockSize

	val scales: Array<SpatialIntArray>
		get() = _scales ?: containerParameters.scales

	val numScales: Int
		get() = scales.size

	val downsamplingBlockSizes: Array<SpatialIntArray>
		get() = fillUpTo(
			(_downsamplingBlockSizes
				?: containerParameters.downsamplingBlockSizes).takeUnless { it.isEmpty() }
				?: arrayOf(blockSize), numScales)

	val reverseArrayAttributes: Boolean
		get() = _reverseArrayAttributes ?: containerParameters.reverseArrayAttributes

	val resolution: SpatialDoubleArray?
		get() = _resolution ?: containerParameters.resolution

	val offset: SpatialDoubleArray?
		get() = _offset ?: containerParameters.offset

	val type: String?
		get() = _type

	val maxNumEntries: IntArray
		get() = _maxNumEntries?.let { fillUpTo(if (it.isEmpty()) intArrayOf(-1) else it, numScales) } ?: fillUpTo(containerParameters.maxNumEntries, numScales)

	val labelBlockLookupN5BlockSize: Int
		get() = _labelBlockLookupN5BlockSize ?: containerParameters.labelBlockLookupN5BlockSize

	val winnerTakesAllDownsampling: Boolean
		get() = _winnerTakesAllDownsampling ?: containerParameters.winnerTakesAllDownsampling

//    @CommandLine.Option(names = ["--overwrite-existing"])
//    var overwriteExisiting: Boolean? = null

}

class SpatialIntArray(private val x: Int, private val y: Int, private val z: Int) {

	constructor(u: Int) : this(u, u, u)

	val array: IntArray
		get() = intArrayOf(x, y, z)

	class Converter : CommandLine.ITypeConverter<SpatialIntArray> {
		override fun convert(value: String): SpatialIntArray {
			return value.split(",").map { it.toInt() }.let { array ->
				when (array.size) {
					1 -> SpatialIntArray(array[0])
					3 -> SpatialIntArray(array[0], array[1], array[2])
					else -> throw Exception("Spatial data must be provided as either single integer or three comma-separated integers but got $value")
				}
			}
		}
	}

	companion object {
		const val PARAM_LABEL = "X,Y,Z|U"
	}
}

class SpatialDoubleArray(private val x: Double, private val y: Double, private val z: Double) {

	constructor(u: Double) : this(u, u, u)

	val array: DoubleArray
		get() = doubleArrayOf(x, y, z)

	class Converter : CommandLine.ITypeConverter<SpatialDoubleArray> {
		override fun convert(value: String): SpatialDoubleArray {
			return value.split(",").map { it.toDouble() }.let { array ->
				when (array.size) {
					1 -> SpatialDoubleArray(array[0])
					3 -> SpatialDoubleArray(array[0], array[1], array[2])
					else -> throw Exception("Spatial data must be provided as either single floating point value or three comma-separated floating point values but got $value")
				}
			}
		}
	}

	companion object {
		const val PARAM_LABEL = "X,Y,Z|U"
	}
}


fun N5Reader.getDoubleArrayAttribute(dataset: String, attribute: String) = try {
	getAttribute(dataset, attribute, DoubleArray::class.java)
} catch (e: ClassCastException) {
	getAttribute(dataset, attribute, LongArray::class.java)?.map { it.toDouble() }?.toDoubleArray()
}

@Throws(IOException::class)
fun N5Writer.setPainteraDataType(group: String, type: String) = setAttribute(group, PAINTERA_DATA_KEY, mapOf(Pair(TYPE_KEY, type)))

const val LABEL_BLOCK_LOOKUP_KEY = "labelBlockLookup"

const val RAW_IDENTIFIER = "raw"

const val LABEL_IDENTIFIER = "label"

const val CHANNEL_IDENTIFIER = "channel"

const val CHANNEL_AXIS_KEY = "channelAxis"

const val CHANNEL_BLOCKSIZE_KEY = "channelBlockSize"

const val RESOLUTION_KEY = "resolution"

const val OFFSET_KEY = "offset"

const val PAINTERA_DATA_KEY = "painteraData"

const val TYPE_KEY = "type"

const val DOWNSAMPLING_FACTORS = "downsamplingFactors"

val TYPE_OPTIONS = listOf("channel", "label", "raw")

class TypeOptions : ArrayList<String>(TYPE_OPTIONS.map { it })

fun <T> fillUpTo(array: Array<T>, size: Int): Array<T> {
	return if (array.size == size)
		array
	else if (array.size < size)
		array + List(size - array.size) { array.last() }
	else
		array.copyOfRange(0, size)
}

fun fillUpTo(array: IntArray, size: Int): IntArray {
	return if (array.size == size)
		array
	else if (array.size < size)
		array + List(size - array.size) { array.last() }
	else
		array.copyOfRange(0, size)
}

fun scaleGroup(group: String, level: Int) = "$group/data/s$level"
