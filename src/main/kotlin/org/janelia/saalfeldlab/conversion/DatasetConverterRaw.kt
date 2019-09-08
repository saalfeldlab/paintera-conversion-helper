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
import org.janelia.saalfeldlab.label.spark.N5Helpers
import org.janelia.saalfeldlab.n5.DataType
import org.janelia.saalfeldlab.n5.GzipCompression
import org.janelia.saalfeldlab.n5.N5FSWriter
import org.janelia.saalfeldlab.n5.spark.N5ConvertSpark
import org.janelia.saalfeldlab.n5.spark.downsample.N5DownsamplerSpark
import org.janelia.saalfeldlab.n5.spark.supplier.N5ReaderSupplier
import org.janelia.saalfeldlab.n5.spark.supplier.N5WriterSupplier
import java.io.IOException
import java.nio.file.Paths
import java.util.Optional

class DatasetConverterRaw(info: DatasetInfo) : DatasetConverter(info) {
    override fun convert(
            sc: JavaSparkContext,
            parameters: DatasetSpecificParameters,
            overwriteExisiting: Boolean
    ) {
        handleRawDatasetInferType(
                sc,
                info,
                parameters.blockSize,
                parameters.scales,
                parameters.downsamplingBlockSizes,
                parameters.revertArrayAttributes ?: false,
                parameters.resolution,
                parameters.offset,
                overwriteExisiting)
    }

    override val type: String
        get() = "raw"

    companion object {

        @Throws(IOException::class)
        private fun handleRawDatasetInferType(
                sc: JavaSparkContext,
                info: DatasetInfo,
                blockSize: IntArray,
                scales: Array<IntArray>,
                downsamplingBlockSizes: Array<IntArray>,
                revertArrayAttributes: Boolean,
                resolution: DoubleArray? = null,
                offset: DoubleArray? = null,
                overwriteExisiting: Boolean = false) {
            when(info.inputContainer.n5Reader()?.getDatasetAttributes(info.inputDataset)?.dataType) {
                DataType.INT8 -> handleRawDataset<ByteType>(sc, info, blockSize, scales, downsamplingBlockSizes, revertArrayAttributes, resolution, offset, overwriteExisiting)
                DataType.UINT8 -> handleRawDataset<UnsignedByteType>(sc, info, blockSize, scales, downsamplingBlockSizes, revertArrayAttributes, resolution, offset, overwriteExisiting)
                DataType.INT16 -> handleRawDataset<ShortType>(sc, info, blockSize, scales, downsamplingBlockSizes, revertArrayAttributes, resolution, offset, overwriteExisiting)
                DataType.UINT16 -> handleRawDataset<UnsignedShortType>(sc, info, blockSize, scales, downsamplingBlockSizes, revertArrayAttributes, resolution, offset, overwriteExisiting)
                DataType.INT32 -> handleRawDataset<IntType>(sc, info, blockSize, scales, downsamplingBlockSizes, revertArrayAttributes, resolution, offset, overwriteExisiting)
                DataType.UINT32 -> handleRawDataset<UnsignedIntType>(sc, info, blockSize, scales, downsamplingBlockSizes, revertArrayAttributes, resolution, offset, overwriteExisiting)
                DataType.INT64 -> handleRawDataset<LongType>(sc, info, blockSize, scales, downsamplingBlockSizes, revertArrayAttributes, resolution, offset, overwriteExisiting)
                DataType.UINT64 -> handleRawDataset<UnsignedLongType>(sc, info, blockSize, scales, downsamplingBlockSizes, revertArrayAttributes, resolution, offset, overwriteExisiting)
                DataType.FLOAT32 -> handleRawDataset<FloatType>(sc, info, blockSize, scales, downsamplingBlockSizes, revertArrayAttributes, resolution, offset, overwriteExisiting)
                DataType.FLOAT64 -> handleRawDataset<DoubleType>(sc, info, blockSize, scales, downsamplingBlockSizes, revertArrayAttributes, resolution, offset, overwriteExisiting)
                null -> throw IOException("Unable to infer data type from dataset `${info.inputDataset}' in container `${info.inputContainer}'")
            }
        }

        @Throws(IOException::class)
        private fun <T> handleRawDataset(
                sc: JavaSparkContext,
                info: DatasetInfo,
                blockSize: IntArray,
                scales: Array<IntArray>,
                downsamplingBlockSizes: Array<IntArray>,
                revertArrayAttributes: Boolean,
                resolution: DoubleArray?,
                offset: DoubleArray?,
                overwriteExisiting: Boolean = false) where T : NativeType<T>, T : RealType<T> {

            val writer = info.outputContainer.n5Writer(DEFAULT_BUILDER)
            writer.createGroup(info.outputGroup)

            setPainteraDataType(writer, info.outputGroup, RAW_IDENTIFIER)

            val dataGroup = Paths.get(info.outputGroup, "data").toString()
            writer.createGroup(dataGroup)
            writer.setAttribute(dataGroup, "multiScale", true)

            val outputDataset = Paths.get(dataGroup, "s0").toString()
            N5ConvertSpark.convert<T, T>(sc,
                    N5ReaderSupplier { info.inputContainer.n5Reader() },
                    info.inputDataset,
                    N5WriterSupplier { info.outputContainer.n5Writer(DEFAULT_BUILDER) },
                    outputDataset,
                    Optional.of(blockSize),
                    Optional.of(GzipCompression()), // TODO pass compression as parameter
                    Optional.empty(),
                    Optional.empty(),
                    overwriteExisiting)

            val downsamplingFactor = DoubleArray(blockSize.size) { 1.0 }

            for (scaleNum in scales.indices) {
                val newScaleDataset = "$dataGroup/s${scaleNum+1}"

                N5DownsamplerSpark.downsample<T>(sc,
                        { N5FSWriter(info.outputContainer, DEFAULT_BUILDER) },
                        "$dataGroup/s${scaleNum+1}",
                        newScaleDataset,
                        scales[scaleNum],
                        downsamplingBlockSizes[scaleNum])

                for (i in downsamplingFactor.indices) {
                    downsamplingFactor[i] *= scales[scaleNum][i].toDouble()
                }
                writer.setAttribute(newScaleDataset, "downsamplingFactors", downsamplingFactor)

            }

            val res = resolution
                    ?: N5Helpers.n5Reader(info.inputContainer).getDoubleArrayAttribute(info.inputDataset, RESOLUTION_KEY)
                    ?: DoubleArray(3) { 1.0 }
            writer.setAttribute("${info.outputGroup}/data", RESOLUTION_KEY, res)

            val off = offset
                    ?: N5Helpers.n5Reader(info.inputContainer).getDoubleArrayAttribute(info.inputDataset, OFFSET_KEY)
                    ?: DoubleArray(3) { 1.0 }
            writer.setAttribute("${info.outputGroup}/data", OFFSET_KEY, off)
        }
    }
}