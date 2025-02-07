package org.janelia.saalfeldlab.conversion.to.paintera

import net.imglib2.type.NativeType
import net.imglib2.type.numeric.RealType
import net.imglib2.type.numeric.integer.*
import net.imglib2.type.numeric.real.DoubleType
import net.imglib2.type.numeric.real.FloatType
import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.conversion.DatasetInfo
import org.janelia.saalfeldlab.conversion.createReader
import org.janelia.saalfeldlab.conversion.createWriter
import org.janelia.saalfeldlab.n5.DataType
import org.janelia.saalfeldlab.n5.GzipCompression
import org.janelia.saalfeldlab.n5.spark.N5ConvertSpark
import org.janelia.saalfeldlab.n5.spark.downsample.N5DownsamplerSpark
import java.io.IOException
import java.nio.file.Paths
import java.util.Optional

class DatasetConverterRaw(info: DatasetInfo) : DatasetConverter(info) {
	override fun convertSpecific(
		sc: JavaSparkContext,
		parameters: DatasetSpecificParameters,
		overwriteExisiting: Boolean
	) {
		handleRawDatasetInferType(
			sc,
			info,
			parameters.blockSize.array,
			parameters.scales.map { it.array }.toTypedArray(),
			parameters.downsamplingBlockSizes.map { it.array }.toTypedArray(),
			overwriteExisiting
		)
	}

	override val type: String
		get() = "raw"
}

@Throws(IOException::class)
private fun handleRawDatasetInferType(
	sc: JavaSparkContext,
	info: DatasetInfo,
	blockSize: IntArray,
	scales: Array<IntArray>,
	downsamplingBlockSizes: Array<IntArray>,
	overwriteExisiting: Boolean = false
) {
	when (createReader(info.inputContainer)?.getDatasetAttributes(info.inputDataset)?.dataType) {
		DataType.INT8 -> handleRawDataset<ByteType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
		DataType.UINT8 -> handleRawDataset<UnsignedByteType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
		DataType.INT16 -> handleRawDataset<ShortType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
		DataType.UINT16 -> handleRawDataset<UnsignedShortType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
		DataType.INT32 -> handleRawDataset<IntType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
		DataType.UINT32 -> handleRawDataset<UnsignedIntType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
		DataType.INT64 -> handleRawDataset<LongType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
		DataType.UINT64 -> handleRawDataset<UnsignedLongType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
		DataType.FLOAT32 -> handleRawDataset<FloatType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
		DataType.FLOAT64 -> handleRawDataset<DoubleType>(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
		else -> throw IOException("Unable to infer data type from dataset `${info.inputDataset}' in container `${info.inputContainer}'")
	}
}

@Throws(IOException::class)
fun <T> handleRawDataset(
	sc: JavaSparkContext,
	info: DatasetInfo,
	blockSize: IntArray,
	scales: Array<IntArray>,
	downsamplingBlockSizes: Array<IntArray>,
	overwriteExisiting: Boolean = false
) where T : NativeType<T>, T : RealType<T> {

	val writer = createWriter(info.outputContainer)
	writer.createGroup(info.outputGroup)

	val dataGroup = Paths.get(info.outputGroup, "data").toString()
	writer.createGroup(dataGroup)
	writer.setAttribute(dataGroup, "multiScale", true)

	val outputDataset = scaleGroup(info.outputGroup, 0).also { writer.createGroup(it) }
	if (info.inputSameAsOutput()) {
		println("Skip conversion of s0 because it is given as an input")
	} else {
		N5ConvertSpark.convert<T, T>(
			sc,
			{ createReader(info.inputContainer) },
			info.inputDataset,
			{ createWriter(info.outputContainer) },
			outputDataset,
			Optional.of(blockSize),
			Optional.of(GzipCompression()), // TODO pass compression as parameter
			Optional.empty(),
			Optional.empty(),
			overwriteExisiting
		)
	}

	val downsamplingFactor = DoubleArray(blockSize.size) { 1.0 }

	for ((scaleNum, scale) in scales.withIndex()) {
		val newScaleDataset = "$dataGroup/s${scaleNum + 1}"

		N5DownsamplerSpark.downsample<T>(
			sc,
			{ createWriter(info.outputContainer) },
			"$dataGroup/s$scaleNum",
			newScaleDataset,
			scales[scaleNum],
			downsamplingBlockSizes[scaleNum]
		)

		for (i in downsamplingFactor.indices)
			downsamplingFactor[i] *= scale[i].toDouble()

	}
}
