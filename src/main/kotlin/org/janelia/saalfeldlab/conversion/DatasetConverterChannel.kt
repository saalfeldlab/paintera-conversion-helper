package org.janelia.saalfeldlab.conversion

import net.imglib2.type.NativeType
import net.imglib2.type.numeric.RealType
import net.imglib2.type.numeric.integer.ByteType
import net.imglib2.type.numeric.integer.IntType
import net.imglib2.type.numeric.integer.LongType
import net.imglib2.type.numeric.integer.ShortType
import net.imglib2.type.numeric.integer.UnsignedByteType
import net.imglib2.type.numeric.integer.UnsignedIntType
import net.imglib2.type.numeric.integer.UnsignedLongType
import net.imglib2.type.numeric.integer.UnsignedShortType
import net.imglib2.type.numeric.real.DoubleType
import net.imglib2.type.numeric.real.FloatType
import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.n5.DataType
import java.io.IOException

class DatasetConverterChannel(info: DatasetInfo) : DatasetConverter(info) {

    override fun convertSpecific(sc: JavaSparkContext, parameters: DatasetSpecificParameters, overwriteExisiting: Boolean) {
        val blockSize = parameters.blockSize
        val scales = parameters.scales
        val downsamplingBlockSizes = parameters.downsamplingBlockSizes
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

    datasetInfo.outputContainer.n5Writer(DEFAULT_BUILDER).setAttribute(datasetInfo.outputGroup, CHANNEL_AXIS_KEY, channelAxis)

}