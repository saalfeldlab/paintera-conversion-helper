package org.janelia.saalfeldlab.conversion.to.paintera

import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.conversion.DatasetInfo
import org.janelia.saalfeldlab.conversion.createReader
import org.janelia.saalfeldlab.conversion.createWriter
import java.io.IOException

class DatasetConverterChannel(info: DatasetInfo) : DatasetConverter(info) {

	override fun convertSpecific(sc: JavaSparkContext, parameters: DatasetSpecificParameters, overwriteExisiting: Boolean) {
		/* the dataType is only validated here; handleRawDataset resolves the actual type at runtime */
		if (createReader(info.inputContainer)?.getDatasetAttributes(info.inputDataset)?.dataType !in SUPPORTED_RAW_TYPES)
			throw IOException("Unable to infer data type from dataset `${info.inputDataset}' in container `${info.inputContainer}'")
		handleChannelDataset(
			sc,
			info,
			parameters.blockSize.array,
			parameters.scales.map { it.array }.toTypedArray(),
			parameters.downsamplingBlockSizes.map { it.array }.toTypedArray(),
			overwriteExisiting
		)
	}

	override val type: String = "channel"

}

@Throws(IOException::class)
private fun handleChannelDataset(
	sc: JavaSparkContext,
	datasetInfo: DatasetInfo,
	blockSize: IntArray,
	scales: Array<IntArray>,
	downsamplingBlockSizes: Array<IntArray>,
	overwriteExisting: Boolean
) {
	val attributes = datasetInfo.attributes
	// TODO make these two configurable:
	val channelAxis = attributes.numDimensions - 1
	val channelBlockSize = 1

	handleRawDataset(
		sc,
		datasetInfo,
		blockSize + intArrayOf(channelBlockSize),
		scales.map { it + intArrayOf(1) }.toTypedArray(),
		downsamplingBlockSizes.map { it + intArrayOf(channelBlockSize) }.toTypedArray(),
		overwriteExisting
	)

	createWriter(datasetInfo.outputFormat, datasetInfo.outputContainer).setAttribute(datasetInfo.outputGroup, CHANNEL_AXIS_KEY, channelAxis)

}
