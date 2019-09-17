package org.janelia.saalfeldlab.paintera.conversion.to.paintera

import com.google.gson.GsonBuilder
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.label.spark.N5Helpers
import org.janelia.saalfeldlab.n5.N5FSWriter
import org.janelia.saalfeldlab.n5.N5Reader
import org.janelia.saalfeldlab.n5.N5Writer
import org.janelia.saalfeldlab.paintera.conversion.ConversionException
import org.janelia.saalfeldlab.paintera.conversion.DatasetInfo
import org.janelia.saalfeldlab.paintera.conversion.NoSparkMasterSpecified
import org.janelia.saalfeldlab.paintera.conversion.PainteraConvert
import org.slf4j.LoggerFactory
import picocli.CommandLine
import java.io.File
import java.io.IOException
import java.lang.invoke.MethodHandles
import java.util.concurrent.Callable
import kotlin.system.exitProcess

class ToPainteraData {

    companion object {

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

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
            aliases = ["tp"],
            usageHelpWidth = 120,
            header = [
                "" +
                        "Converts arbitrary 3D label and single- or multi-channel raw datasets " +
                        "in N5 or HDF5 containers into a Paintera-friendly format.  ",
                ""],
            description = [
                "",
                "" +
                        "Converts arbitrary 3D label and single- or multi-channel raw datasets in N5 or HDF5 containers into a Paintera-friendly format (https://github.com/saalfeldlab/paintera#paintera-data-format).  " +
                        "A Paintera-friendly format is a group (referred to as \"paintera group\" in the following) inside an N5 container with a multi-scale representation (mipmap pyramid) in the `data' sub-group. " +
                        "The `data' sub-group contains datasets s0 ... sN, where s0 is the highest resolution dataset and sN is the lowest resolution (most downsampled) dataset.  " +
                        "Each dataset sX has an attribute `\"downsamplingFactors\":[X, Y, Z]' relative to s0, e.g. `\"downsamplingFactors\":[16.0,16.0,2.0]'.  " +
                        "If not specified, `\"downsamplingFactors\":[1.0, 1.0, 1.0]' is assumed (this makes sense only for s0).  " +
                        "Unless the `--winner-takes-all-downsampling' option is specified, label data is converted and downsampled with a non-scalar summarizing label type (https://github.com/saalfeldlab/paintera#label-multisets).  " +
                        "The highest resolution label dataset can be extracted as scalar UINT64 label type with the `to-scalar' sub-command.  " +
                        "The paintera group has a \"painteraData\" attribute to specify the type of the dataset, i.e. `\"painteraData\":{\"type\":\"\$TYPE\"}', " +
                        "where TYPE is one of {channel, label, raw}.",
                "",
                "" +
                        "Label data paintera groups have additional sub-groups to store unique lists of label ids for each block (`unique-labels') per each scale level, " +
                        "an index of containing blocks for each label id (`label-to-block-mapping') per each scale level, " +
                        "and a lookup table for manual agglomeration of fragments (`fragment-segment-assignment').  " +
                        "Currently, the lookup table cannot be provided during conversion and will be populated when using Paintera.  " +
                        "A mandatory attribute `maxId' in the paintera group keeps track of the largest label id that has been used for a dataset.  " +
                        "The `\"labelBlockLookup\"' attribute specifies the type of index stored in `label-to-block-mapping'.",
                "",
                "" +
                        "Conversion options can be set at (a) the global level, (b) at the N5/HDF5 container level, or (c) at a dataset level.  " +
                        "More specific options take precedence over more general option if specified, in particular (b) overrides (a) and (c) overrides (b).  " +
                        "Options that override options set at a more general level are prefixed with `--container' and `--dataset' for (b) and (c), respectively.  " +
                        "For example, the downsampling factors/scales can be set with the `--scale' option at the global level and overriden with the " +
                        "`--container-scale' option at the container level or the `--dataset-scale' option at the dataset level.",
                "",
                "" +
                        "The following parameters of conversion can be set at global, container, or dataset level:",
                "",
                "    Scales:  A list of 3-tuples of integers  or single integers that determine the downsampling and the number of mipmap levels.",
                "    Block Size:  A 3-tuple of integers that specifies the block size of s0 data set that is being copied (or copy-converted). Defaults to (64, 64, 64)",
                "    Downsampling block sizes:  A list of 3-tuples of integers that specify the block size at each scale level. " +
                        "If fewer downsampling block sizes than scales are specified, the unspecified downsampling block sizes default to the block size of the lowest resolution dataset sN for which a block size is specified.",
                "    Resolution:  3-tuple of floating point values to specify resolution (physical extent) of a voxel.  " +
                        "Defaults to (1.0, 1.0, 1.0) or is inferred from the input data if available, if not specified.",
                "    Offset:  3-tuple of floating point values to specify offset of the center of the top-left voxel of the data in some arbitrary coordinate space defined by the resolution.  " +
                        "Defaults to (0.0, 0.0, 0.0) or is inferred from the input data if available, if not specified.",
                "    Revert array attributes:  Revert array attributes (currently only resolution and offset) when read from input data, e.g. (3.0, 2.0, 1.0) will become (1.0, 2.0, 3.0).",
                "    Label only:",
                "        Winner takes all downsampling:  Use gerrymandering scalar label type for downsampling instead of non-scalar, summarizing label type (https://github.com/saalfeldlab/paintera#label-multisets).",
                "        Label block lookup block size:  A single integer that specifies the block size for the index stored in `label-to-block-mapping' that is stored as N5 dataset for each scale level.",
                "",
                "Example command for sample A of the CREMI challenge (https://cremi.org/static/data/sample_A_20160501.hdf):",
                "",
                """
paintera-convert to-paintera \
  --scale 2,2,1 2,2,1 2,2,1 2 2 \
  --revert-array-attributes \
  --output-container=paintera-converted.n5 \
  --container=sample_A_20160501.hdf \
    -d volumes/raw \
      --target-dataset=volumes/raw2 \
      --dataset-scale 3,3,1 3,3,1 2 2 \
      --dataset-resolution 4,4,40.0 \
    -d volumes/labels/neuron_ids \
""",
                "",
                "Options:",
                ""])
    class Parameters : Callable<Int> {
        @CommandLine.ArgGroup(exclusive = false, multiplicity = "0..1")
        var parameters: GlobalParameters = GlobalParameters()

        @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*")
        lateinit var containers: Array<ContainerParameters>

        @CommandLine.Option(names = ["--overwrite-existing"], defaultValue = "false")
        var overwriteExisting: Boolean = false

        @CommandLine.Option(names = ["--help"], help = true, usageHelp = true)
        var helpRequested: Boolean = false

        @CommandLine.Option(names = ["--output-container"], required = true, paramLabel = "OUTPUT_CONTAINER")
        lateinit var _outputContainer: String

        val outputContainer: String
            get() = File(_outputContainer).absolutePath

        @CommandLine.Option(
                names = ["--spark-master"],
                required = false)
        var sparkMaster: String? = null

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
                            inputContainer = container.container.absolutePath,
                            inputDataset = dataset.dataset,
                            outputContainer = outputContainer,
                            outputGroup = dataset.targetDataset)
                    try {
                        info.ensureInput()
                        info.ensureOutput(overwriteExisting)
                        if (info in datasets)
                            throw ConversionException("Dataset specified multiple times: `$info'")
                        val converter = DatasetConverter[info, dataset.parameters.type ?: info.type] ?: throw ConversionException("Do not know how to convert dataset of type `${dataset.parameters.type}': `$info'")
                        datasets[info] = Pair(converter, dataset.parameters)
                    } catch (e: ConversionException) {
                        exceptions += e
                    }
                }
            }

            if (exceptions.isNotEmpty()) {
                System.err.println("Invalid options:")
                exceptions.forEach { System.err.println(it.message) }
                return exceptions[0].exitCode
            }

            return try {
                val conf = SparkConf().setAppName(MethodHandles.lookup().lookupClass().simpleName)
                sparkMaster?.let { conf.setMaster(it) }
                try {
                    if (conf["spark.master"] === null)
                        throw NoSparkMasterSpecified("--spark-master")
                } catch (_: NoSuchElementException) {
                    throw NoSparkMasterSpecified("--spark-master")
                }
                JavaSparkContext(conf).use { sc ->
                    datasets.forEach { dataset, (converter, parameters) ->
                        println("Converting dataset `$dataset'")
                        converter.convert(sc, parameters, overwriteExisting)
                    }
                }
                0
            } catch (conversionError: ConversionException) {
                System.err.println(conversionError.message)
                conversionError.exitCode
            } catch (error: Exception) {
                System.err.println("Unable to convert into Paintera dataset: ${error.message}")
                PainteraConvert.EXIT_CODE_EXECUTION_EXCEPTION
            }

        }


    }

}

class GlobalParameters : Callable<Unit> {
    // TODO use custom class instead of IntArray
    @CommandLine.Option(
            names = ["--block-size"],
            paramLabel = SpatialIntArray.PARAM_LABEL,
            defaultValue = "64,64,64",
            converter = [SpatialIntArray.Converter::class])
    private lateinit var _blockSize: SpatialIntArray

    @CommandLine.Option(
            names =  ["--scale"],
            arity = "1..*",
            description = ["Relative downsampling factors for each level in the format x,y,z, where x,y,z are integers. Single integers u are interpreted as u,u,u"],
            converter = [SpatialIntArray.Converter::class],
            paramLabel = SpatialIntArray.PARAM_LABEL)
    private lateinit var _scales: Array<SpatialIntArray>

    @CommandLine.Option(
            names = ["--downsample-block-sizes"],
            arity = "1..*",
            converter = [SpatialIntArray.Converter::class],
            paramLabel = SpatialIntArray.PARAM_LABEL)
    private lateinit var _downsamplingBlockSizes: Array<SpatialIntArray>

    @CommandLine.Option(names = ["--revert-array-attributes"], defaultValue = "false")
    var revertArrayAttributes: Boolean = false

    @CommandLine.Option(names = ["--resolution"], converter = [SpatialDoubleArray.Converter::class], paramLabel = SpatialDoubleArray.PARAM_LABEL)
    var resolution: SpatialDoubleArray? = null

    @CommandLine.Option(names = ["--offset"], converter = [SpatialDoubleArray.Converter::class], paramLabel = SpatialDoubleArray.PARAM_LABEL)
    var offset: SpatialDoubleArray? = null

    @CommandLine.Option(names = ["--max-num-entries", "-m"], arity = "1..*", paramLabel = "N")
    private var _maxNumEntries: IntArray? = null

    @CommandLine.Option(names = ["--label-block-lookup-n5-block-size"], defaultValue = "10000", paramLabel = "N")
    var labelBlockLookupN5BlockSize: Int = 10000
        private set

    @CommandLine.Option(names = ["--winner-takes-all-downsampling"], defaultValue = "false")
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
    private lateinit var _container: File

    @CommandLine.ArgGroup(exclusive = false)
    var parameters: ContainerSpecificParameters = ContainerSpecificParameters()
        private set

    @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*")
    private lateinit var _datasets: Array<DatasetParameters>

    val container: File
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
            paramLabel = SpatialIntArray.PARAM_LABEL,
            defaultValue = "64,64,64",
            converter = [SpatialIntArray.Converter::class])
    private var _blockSize: SpatialIntArray? = null

    @CommandLine.Option(
            names =  ["--container-scale"],
            arity = "1..*",
            description = ["Relative downsampling factors for each level in the format x,y,z, where x,y,z are integers. Single integers u are interpreted as u,u,u"],
            converter = [SpatialIntArray.Converter::class],
            paramLabel = SpatialIntArray.PARAM_LABEL)
    private var _scales: Array<SpatialIntArray>? = null

    @CommandLine.Option(
            names = ["--container-downsample-block-sizes"],
            arity = "1..*",
            converter = [SpatialIntArray.Converter::class],
            paramLabel = SpatialIntArray.PARAM_LABEL)
    private var _downsamplingBlockSizes: Array<SpatialIntArray>? = null

    @CommandLine.Option(names = ["--container-revert-array-attributes"])
    private var _revertArrayAttributes: Boolean? = null

    @CommandLine.Option(names = ["--container-resolution"], converter = [SpatialDoubleArray.Converter::class], paramLabel = SpatialDoubleArray.PARAM_LABEL)
    private var _resolution: SpatialDoubleArray? = null

    @CommandLine.Option(names = ["--container-offset"], converter = [SpatialDoubleArray.Converter::class], paramLabel = SpatialDoubleArray.PARAM_LABEL)
    private var _offset: SpatialDoubleArray? = null

    @CommandLine.Option(names = ["--container-max-num-entries"], arity = "1..*", paramLabel = "N")
    private var _maxNumEntries: IntArray? = null

    @CommandLine.Option(names = ["--container-label-block-lookup-n5-block-size"], paramLabel = "N")
    private var _labelBlockLookupN5BlockSize: Int? = null

    @CommandLine.Option(names = ["--container-winner-takes-all-downsampling"])
    private var _winnerTakesAllDownsampling: Boolean? = null

    val blockSize: SpatialIntArray
        get() = _blockSize ?: globalParameters.blockSize

    val scales: Array<SpatialIntArray>
        get() = _scales ?: globalParameters.scales

    val numScales: Int
        get() = scales.size

    val downsamplingBlockSizes: Array<SpatialIntArray>
        get() = fillUpTo((_downsamplingBlockSizes
                ?: globalParameters.downsamplingBlockSizes).takeUnless { it.isEmpty() }
                ?: arrayOf(blockSize), numScales)

    val revertArrayAttributes: Boolean
        get() = _revertArrayAttributes ?: globalParameters.revertArrayAttributes

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
            paramLabel = SpatialIntArray.PARAM_LABEL,
            converter = [SpatialIntArray.Converter::class])
    private var _blockSize: SpatialIntArray? = null

    @CommandLine.Option(
            names =  ["--dataset-scale"],
            arity = "1..*",
            description = ["Relative downsampling factors for each level in the format x,y,z, where x,y,z are integers. Single integers u are interpreted as u,u,u"],
            converter = [SpatialIntArray.Converter::class],
            paramLabel = SpatialIntArray.PARAM_LABEL)
    private var _scales: Array<SpatialIntArray>? = null

    @CommandLine.Option(
            names = ["--dataset-downsample-block-sizes"],
            arity = "1..*",
            description = ["Relative downsampling factors for each level in the format x,y,z, where x,y,z are integers. Single integers u are interpreted as u,u,u"],
            converter = [SpatialIntArray.Converter::class],
            paramLabel = SpatialIntArray.PARAM_LABEL)
    private var _downsamplingBlockSizes: Array<SpatialIntArray>? = null

    @CommandLine.Option(names = ["--dataset-revert-array-attributes"])
    private var _revertArrayAttributes: Boolean? = null

    @CommandLine.Option(names = ["--dataset-resolution"], converter = [SpatialDoubleArray.Converter::class], paramLabel = SpatialDoubleArray.PARAM_LABEL)
    private var _resolution: SpatialDoubleArray? = null

    @CommandLine.Option(names = ["--dataset-offset"], converter = [SpatialDoubleArray.Converter::class], paramLabel = SpatialDoubleArray.PARAM_LABEL)
    private var _offset: SpatialDoubleArray? = null

    @CommandLine.Option(names = ["--type"], completionCandidates = TypeOptions::class, required = false, paramLabel = "TYPE")
    private var _type: String? = null

    @CommandLine.Option(names = ["--dataset-max-num-entries"], arity = "1..*", paramLabel = "N")
    private var _maxNumEntries: IntArray? = null

    @CommandLine.Option(names = ["--dataset-label-block-lookup-n5-block-size"], paramLabel = "N")
    private var _labelBlockLookupN5BlockSize: Int? = null

    @CommandLine.Option(names = ["--dataset-winner-takes-all-downsampling"])
    private var _winnerTakesAllDownsampling: Boolean? = null

    val blockSize: SpatialIntArray
        get() = _blockSize ?: containerParameters.blockSize

    val scales: Array<SpatialIntArray>
        get() = _scales ?: containerParameters.scales

    val numScales: Int
        get() = scales.size

    val downsamplingBlockSizes: Array<SpatialIntArray>
        get() = fillUpTo((_downsamplingBlockSizes
                ?: containerParameters.downsamplingBlockSizes).takeUnless { it.isEmpty() }
                ?: arrayOf(blockSize), numScales)

    val revertArrayAttributes: Boolean
        get() = _revertArrayAttributes ?: containerParameters.revertArrayAttributes

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

class SpatialIntArray (private val x: Int, private val y: Int, private val z: Int) {

    constructor(u: Int): this(u, u, u)

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

class SpatialDoubleArray (private val x: Double, private val y: Double, private val z: Double) {

    constructor(u: Double): this(u, u, u)

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


fun String.n5Reader() = N5Helpers.n5Reader(this)

fun String.n5Writer(builder: GsonBuilder? = null) = builder?.let { N5FSWriter(this, it) } ?: N5FSWriter(this)

fun N5Reader.getDoubleArrayAttribute(dataset: String, attribute: String) = try {
    getAttribute(dataset, attribute, DoubleArray::class.java)
} catch (e: ClassCastException) {
    getAttribute(dataset, attribute, LongArray::class.java)?.map { it.toDouble() }?.toDoubleArray()
}

@Throws(IOException::class)
fun N5Writer.setPainteraDataType(group: String, type: String) = setAttribute(group, PAINTERA_DATA_KEY, mapOf(Pair(TYPE_KEY, type)))

val DEFAULT_BUILDER = GsonBuilder().setPrettyPrinting().disableHtmlEscaping()

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
