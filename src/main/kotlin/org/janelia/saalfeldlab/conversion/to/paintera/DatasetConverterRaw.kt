package org.janelia.saalfeldlab.conversion.to.paintera

import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.conversion.DatasetInfo
import org.janelia.saalfeldlab.conversion.createReader
import org.janelia.saalfeldlab.conversion.createWriter
import org.janelia.saalfeldlab.n5.DataType
import org.janelia.saalfeldlab.n5.spark.N5ConvertSpark
import org.janelia.saalfeldlab.n5.spark.downsample.N5DownsamplerSpark
import org.janelia.scicomp.n5.zstandard.ZstandardCompression
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

internal val SUPPORTED_RAW_TYPES = setOf(
	DataType.INT8, DataType.UINT8, DataType.INT16, DataType.UINT16,
	DataType.INT32, DataType.UINT32, DataType.INT64, DataType.UINT64,
	DataType.FLOAT32, DataType.FLOAT64,
)

@Throws(IOException::class)
private fun handleRawDatasetInferType(
	sc: JavaSparkContext,
	info: DatasetInfo,
	blockSize: IntArray,
	scales: Array<IntArray>,
	downsamplingBlockSizes: Array<IntArray>,
	overwriteExisiting: Boolean = false
) {
	/* the dataType is only validated here; the spark converters resolve the actual type at runtime */
	if (createReader(info.inputContainer)?.getDatasetAttributes(info.inputDataset)?.dataType !in SUPPORTED_RAW_TYPES)
		throw IOException("Unable to infer data type from dataset `${info.inputDataset}' in container `${info.inputContainer}'")
	handleRawDataset(sc, info, blockSize, scales, downsamplingBlockSizes, overwriteExisiting)
}

@Throws(IOException::class)
fun handleRawDataset(
	sc: JavaSparkContext,
	info: DatasetInfo,
	blockSize: IntArray,
	scales: Array<IntArray>,
	downsamplingBlockSizes: Array<IntArray>,
	overwriteExisiting: Boolean = false
) {

	val writer = createWriter(info.outputFormat, info.outputContainer)
	writer.createGroup(info.outputGroup)

	val dataGroup = Paths.get(info.outputGroup, "data").toString()
	writer.createGroup(dataGroup)
	writer.setAttribute(dataGroup, "multiScale", true)

	val outputDataset = scaleGroup(info.outputGroup, 0).also { writer.createGroup(it) }
	if (info.inputSameAsOutput()) {
		println("Skip conversion of s0 because it is given as an input")
	} else {
			/* type args are erased; the actual type is resolved at runtime from the dataset's DataType */
			N5ConvertSpark.convert<Nothing, Nothing>(
			sc,
			{ createReader(info.inputContainer) },
			info.inputDataset,
			{ createWriter(info.outputFormat, info.outputContainer) },
			outputDataset,
			Optional.of(blockSize),
			Optional.of(ZstandardCompression()), // TODO pass compression as parameter
			Optional.empty(),
			Optional.empty(),
			overwriteExisiting
		)
	}

	val downsamplingFactor = DoubleArray(blockSize.size) { 1.0 }

	for ((scaleNum, scale) in scales.withIndex()) {
		val newScaleDataset = "$dataGroup/s${scaleNum + 1}"

		N5DownsamplerSpark.downsample<Nothing>(
			sc,
			{ createWriter(info.outputFormat, info.outputContainer) },
			"$dataGroup/s$scaleNum",
			newScaleDataset,
			scales[scaleNum],
			downsamplingBlockSizes[scaleNum]
		)

		for (i in downsamplingFactor.indices)
			downsamplingFactor[i] *= scale[i].toDouble()

	}
}
