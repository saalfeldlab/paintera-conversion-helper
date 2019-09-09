package org.janelia.saalfeldlab.conversion

import com.google.gson.GsonBuilder
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.label.spark.N5Helpers
import org.janelia.saalfeldlab.n5.N5FSWriter
import org.janelia.saalfeldlab.n5.N5Reader
import org.janelia.saalfeldlab.n5.N5Writer
import org.slf4j.LoggerFactory
import picocli.CommandLine
import java.io.File
import java.io.IOException
import java.lang.ClassCastException
import java.lang.invoke.MethodHandles
import java.util.concurrent.Callable
import kotlin.system.exitProcess

class PainteraConvert {

    companion object {

        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        @JvmStatic
        fun main(argv: Array<String>) {
            val args = PainteraConvertParameters()
            val cl = CommandLine(args)
            val exitCode = cl.execute(*argv)

            if (exitCode != 0)
                exitProcess(255 - exitCode)

            if (args.versionOrHelpRequested) {
                if (args.versionRequested)
                    println(Version.VERSION_STRING)
                exitProcess(0)
            }

            val datasets: MutableMap<DatasetInfo, Pair<DatasetConverter, DatasetSpecificParameters>> = mutableMapOf()
            val exceptions = mutableListOf<ConversionException>()
            for (container in args.containers) {
                for (dataset in container.datasets) {
                    val info = DatasetInfo(
                            inputContainer = container.container,
                            inputDataset = dataset.dataset,
                            outputContainer = args.outputContainer,
                            outputGroup = dataset.targetDataset)
                    try {
                        info.ensureInput()
                        info.ensureOutput(args.overwriteExisting)
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
//                LOG.error("Invalid options:")
//                exceptions.forEach { LOG.error("{}", it) }
                println("Invalid options:")
                exceptions.forEach { println(it.message) }
                exitProcess(1)
            }

            val conf = SparkConf().setAppName(MethodHandles.lookup().lookupClass().simpleName)
            JavaSparkContext(conf).use { sc ->
                datasets.forEach { dataset, (converter, parameters) ->
                    println("Converting dataset `$dataset'")
                    converter.convert(sc, parameters, args.overwriteExisting)
                }
            }

        }
    }

}

class ArrayOfSpatialIntArrayConverter : CommandLine.ITypeConverter<Array<IntArray>> {
    override fun convert(value: String?): Array<IntArray>? {
        return value
                ?.split(" ")
                ?.map { singleArrayConverter.convert(it) ?: throw Exception() }
                ?.toTypedArray()
    }

    private val singleArrayConverter = SpatialIntArrayConverter()

}

class SpatialIntArrayConverter : CommandLine.ITypeConverter<IntArray> {
    override fun convert(value: String?): IntArray? {
        return value
                ?.split(",")
                ?.map { it.toInt() }
                ?.let { if (it.size == 3) it.toIntArray() else if (it.size == 1) IntArray(3) { _ -> it[0] } else throw Exception() }
    }
}

class SpatialArrayConverter : CommandLine.ITypeConverter<DoubleArray> {
    override fun convert(value: String?): DoubleArray? {
        return value
                ?.split(",")
                ?.map { it.toDouble() }
                ?.let { if (it.size == 3) it.toDoubleArray() else if (it.size == 1) DoubleArray(3) { _ -> it[0] } else throw Exception() }
    }

}

class GlobalParameters : Callable<Unit> {
    @CommandLine.Option(names = ["--block-size"], defaultValue = "64,64,64", split = ",")
    private lateinit var _blockSize: IntArray

    @CommandLine.Option(
            names =  ["--scale"],
            arity = "1..*", description = ["Relative downsampling factors for each level in the format x,y,z, where x,y,z are integers. Single integers u are interpreted as u,u,u"],
            converter = [SpatialIntArrayConverter::class])
    private lateinit var _scales: Array<IntArray>

    @CommandLine.Option(names = ["--downsample-block-sizes"], arity = "1..*")
    private lateinit var _downsamplingBlockSizes: Array<IntArray>

    @CommandLine.Option(names = ["--revert-array-attributes"], defaultValue = "false")
    var revertArrayAttributes: Boolean = false

    @CommandLine.Option(names = ["--resolution"], converter = [SpatialArrayConverter::class])
    var resolution: DoubleArray? = null

    @CommandLine.Option(names = ["--offset"], converter = [SpatialArrayConverter::class])
    var offset: DoubleArray? = null

    @CommandLine.Option(names = ["--max-num-entries", "-m"], arity = "1..*")
    private var _maxNumEntries: IntArray? = null

    @CommandLine.Option(names = ["--label-block-lookup-n5-block-size"], defaultValue = "10000")
    var labelBlockLookupN5BlockSize: Int = 10000
        private set

    @CommandLine.Option(names = ["--winner-takes-all-downsampling"], defaultValue = "false")
    var winnerTakesAllDownsampling: Boolean = false
        private set

    val blockSize: IntArray
        get() = _blockSize

    val scales: Array<IntArray>
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

@CommandLine.Command(name = "paintera-convert")
class PainteraConvertParameters : Callable<Unit> {
    @CommandLine.ArgGroup(exclusive = false, multiplicity = "0..1")
    var parameters: GlobalParameters = GlobalParameters()

    @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*")
    lateinit var containers: Array<ContainerParameters>

    @CommandLine.Option(names = ["--overwrite-existing"], defaultValue = "false")
    var overwriteExisting: Boolean = false

    @CommandLine.Option(names = ["--help"], help = true, usageHelp = true)
    var helpRequested: Boolean = false

    @CommandLine.Option(names = ["--version"], help = true, versionHelp = true)
    var versionRequested: Boolean = false

    @CommandLine.Option(names = ["--output-container"], required = true)
    lateinit var _outputContainer: String

    val versionOrHelpRequested: Boolean
        get() = helpRequested || versionRequested

    val outputContainer: String
        get() = File(_outputContainer).absolutePath

    override fun call() {
        parameters.call()
        if (!versionOrHelpRequested)
            containers.forEach { it.parameters.initGlobalParameters(parameters); it.call() }
    }


}

class ContainerParameters : Callable<Unit> {

    @CommandLine.Option(names = ["--container"])
    private lateinit var _container: String

    @CommandLine.ArgGroup(exclusive = false)
    var parameters: ContainerSpecificParameters = ContainerSpecificParameters()
        private set

    @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*")
    private lateinit var _datasets: Array<DatasetParameters>

    val container: String
        get() = File(_container).absolutePath

    val datasets: Array<DatasetParameters>
        get() = _datasets

    override fun call() {
        _container = File(container).absolutePath
        datasets.forEach { it.parameters.initGlobalParameters(parameters) }
    }

}

class DatasetParameters {

    @CommandLine.Option(names = ["--dataset", "-d"], required = true)
    private lateinit var _dataset: String

    @CommandLine.Option(names = ["--target-dataset"])
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

    @CommandLine.Option(names = ["--container-block-size"])
    private var _blockSize: IntArray? = null

    @CommandLine.Option(
            names =  ["--container-scale"],
            arity = "1..*",
            description = ["Relative downsampling factors for each level in the format x,y,z, where x,y,z are integers. Single integers u are interpreted as u,u,u"],
            converter = [SpatialIntArrayConverter::class])
    private var _scales: Array<IntArray>? = null

    @CommandLine.Option(names = ["--container-downsample-block-sizes"], arity = "1..*")
    private var _downsamplingBlockSizes: Array<IntArray>? = null

    @CommandLine.Option(names = ["--container-revert-array-attributes"])
    private var _revertArrayAttributes: Boolean? = null

    @CommandLine.Option(names = ["--container-resolution"])
    private var _resolution: DoubleArray? = null

    @CommandLine.Option(names = ["--container-offset"])
    private var _offset: DoubleArray? = null

    @CommandLine.Option(names = ["--container-max-num-entries"], arity = "1..*")
    private var _maxNumEntries: IntArray? = null

    @CommandLine.Option(names = ["--container-label-block-lookup-n5-block-size"])
    private var _labelBlockLookupN5BlockSize: Int? = null

    @CommandLine.Option(names = ["--container-winner-takes-all-downsampling"])
    private var _winnerTakesAllDownsampling: Boolean? = null

    val blockSize: IntArray
        get() = _blockSize ?: globalParameters.blockSize

    val scales: Array<IntArray>
        get() = _scales ?: globalParameters.scales

    val numScales: Int
        get() = scales.size

    val downsamplingBlockSizes: Array<IntArray>
        get() = fillUpTo((_downsamplingBlockSizes ?: globalParameters.downsamplingBlockSizes).takeUnless { it.isEmpty() } ?: arrayOf(blockSize), numScales)

    val revertArrayAttributes: Boolean
        get() = _revertArrayAttributes ?: globalParameters.revertArrayAttributes

    val resolution: DoubleArray?
        get() = _resolution ?: globalParameters.resolution

    val offset: DoubleArray?
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

    @CommandLine.Option(names = ["--dataset-block-size"])
    private var _blockSize: IntArray? = null

    @CommandLine.Option(
            names =  ["--dataset-scale"],
            arity = "1..*",
            description = ["Relative downsampling factors for each level in the format x,y,z, where x,y,z are integers. Single integers u are interpreted as u,u,u"],
            converter = [SpatialIntArrayConverter::class])
    private var _scales: Array<IntArray>? = null

    @CommandLine.Option(names = ["--dataset-downsample-block-sizes"], arity = "1..*")
    private var _downsamplingBlockSizes: Array<IntArray>? = null

    @CommandLine.Option(names = ["--dataset-revert-array-attributes"])
    private var _revertArrayAttributes: Boolean? = null

    @CommandLine.Option(names = ["--dataset-resolution"])
    private var _resolution: DoubleArray? = null

    @CommandLine.Option(names = ["--dataset-offset"])
    private var _offset: DoubleArray? = null

    @CommandLine.Option(names = ["--type"], completionCandidates = TypeOptions::class, required = false)
    private var _type: String? = null

    @CommandLine.Option(names = ["--dataset-max-num-entries"], arity = "1..*")
    private var _maxNumEntries: IntArray? = null

    @CommandLine.Option(names = ["--dataset-label-block-lookup-n5-block-size"])
    private var _labelBlockLookupN5BlockSize: Int? = null

    @CommandLine.Option(names = ["--dataset-winner-takes-all-downsampling"])
    private var _winnerTakesAllDownsampling: Boolean? = null

    val blockSize: IntArray
        get() = _blockSize ?: containerParameters.blockSize

    val scales: Array<IntArray>
        get() = _scales ?: containerParameters.scales

    val numScales: Int
        get() = scales.size

    val downsamplingBlockSizes: Array<IntArray>
        get() = fillUpTo((_downsamplingBlockSizes ?: containerParameters.downsamplingBlockSizes).takeUnless { it.isEmpty() } ?: arrayOf(blockSize), numScales)

    val revertArrayAttributes: Boolean
        get() = _revertArrayAttributes ?: containerParameters.revertArrayAttributes

    val resolution: DoubleArray?
        get() = _resolution ?: containerParameters.resolution

    val offset: DoubleArray?
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
