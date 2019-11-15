package org.janelia.saalfeldlab.paintera.conversion.to.paintera

import net.imglib2.type.NativeType
import net.imglib2.type.numeric.RealType
import net.imglib2.type.numeric.integer.*
import net.imglib2.type.numeric.real.DoubleType
import net.imglib2.type.numeric.real.FloatType
import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.conversion.IncompatibleChannelAxis
import org.janelia.saalfeldlab.n5.DataType
import org.janelia.saalfeldlab.paintera.conversion.DatasetInfo
import java.io.IOException

class DatasetConverterChannel(info: DatasetInfo) : DatasetConverter(info) {

    override fun convertSpecific(sc: JavaSparkContext, parameters: DatasetSpecificParameters, overwriteExisiting: Boolean) {
        val blockSize = parameters.blockSize.array
        val scales = parameters.scales.map { it.array }.toTypedArray()
        val downsamplingBlockSizes = parameters.downsamplingBlockSizes.map { it.array }.toTypedArray()
        when(info.inputContainer.n5Reader()?.getDatasetAttributes(info.inputDataset)?.dataType) {
            DataType.INT8 -> handleChannelDataset<ByteType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
            DataType.UINT8 -> handleChannelDataset<UnsignedByteType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
            DataType.INT16 -> handleChannelDataset<ShortType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
            DataType.UINT16 -> handleChannelDataset<UnsignedShortType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
            DataType.INT32 -> handleChannelDataset<IntType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
            DataType.UINT32 -> handleChannelDataset<UnsignedIntType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
            DataType.INT64 -> handleChannelDataset<LongType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
            DataType.UINT64 -> handleChannelDataset<UnsignedLongType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
            DataType.FLOAT32 -> handleChannelDataset<FloatType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
            DataType.FLOAT64 -> handleChannelDataset<DoubleType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
            null -> throw IOException("Unable to infer data type from dataset `${info.inputDataset}' in container `${info.inputContainer}'")
        }
    }

    override val type: String = "channel"

}

@Throws(IOException::class, IncompatibleChannelAxis::class)
private fun <T> handleChannelDataset(
        sc: JavaSparkContext,
        datasetInfo: DatasetInfo,
        blockSize: IntArray,
        scales: Array<IntArray>,
        downsamplingBlockSizes: Array<IntArray>,
        revertArrayAttributes: Boolean) where T: NativeType<T>, T: RealType<T> {


    val attributes = datasetInfo.attributes
    // TODO make these two configurable:
    val channelAxis = attributes.numDimensions - 1
    val channelBlockSize = 1

//    println("Downsampling ${scales.joinToString(", ", "[", "]") { it.joinToString(", ", "[", "]") }}")
    handleRawDataset<T>(
            sc,
            datasetInfo,
            blockSize + intArrayOf(channelBlockSize),
            scales.map { it + intArrayOf(1) }.toTypedArray(),
            downsamplingBlockSizes.map { it + intArrayOf(channelBlockSize) }.toTypedArray(),
            revertArrayAttributes)

    datasetInfo.outputContainer.n5Writer(defaultGsonBuilder()).setAttribute(datasetInfo.outputGroup, CHANNEL_AXIS_KEY, channelAxis)

}
