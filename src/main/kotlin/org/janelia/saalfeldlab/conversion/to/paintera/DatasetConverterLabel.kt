package org.janelia.saalfeldlab.conversion.to.paintera

import net.imglib2.type.numeric.integer.UnsignedLongType
import org.janelia.saalfeldlab.labels.blocks.n5.LabelBlockLookupFromN5Relative
import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.conversion.DatasetInfo
import org.janelia.saalfeldlab.conversion.createReader
import org.janelia.saalfeldlab.conversion.createWriter
import org.janelia.saalfeldlab.conversion.parseSlicePositions
import org.janelia.saalfeldlab.conversion.slicedInputImgSupplier
import org.janelia.saalfeldlab.label.spark.convert.ConvertToLabelMultisetType
import org.janelia.saalfeldlab.label.spark.downsample.SparkDownsampler
import org.janelia.saalfeldlab.label.spark.exception.InputSameAsOutput
import org.janelia.saalfeldlab.label.spark.exception.InvalidDataType
import org.janelia.saalfeldlab.label.spark.exception.InvalidDataset
import org.janelia.saalfeldlab.label.spark.exception.InvalidN5Container
import org.janelia.saalfeldlab.label.spark.uniquelabels.ExtractUniqueLabelsPerBlock
import org.janelia.saalfeldlab.label.spark.uniquelabels.LabelToBlockMapping
import org.janelia.saalfeldlab.label.spark.uniquelabels.downsample.LabelListDownsampler
import org.janelia.saalfeldlab.n5.DataType
import org.janelia.saalfeldlab.n5.spark.N5ConvertSpark
import org.janelia.saalfeldlab.n5.spark.downsample.N5LabelDownsamplerSpark
import org.janelia.scicomp.n5.zstandard.ZstandardCompression
import java.io.File
import java.io.IOException
import java.nio.file.Paths
import java.util.Optional

class DatasetConverterLabel(info: DatasetInfo) : DatasetConverter(info) {
	override fun convertSpecific(sc: JavaSparkContext, parameters: DatasetSpecificParameters, overwriteExisiting: Boolean) {
		handleLabelDatasetInferType(
			sc,
			info,
			parameters.blockSize.array,
			parameters.scales.map { it.array }.toTypedArray(),
			parameters.downsamplingBlockSizes.map { it.array }.toTypedArray(),
			parameters.maxNumEntries,
			parameters.reverseArrayAttributes,
			parameters.winnerTakesAllDownsampling,
			parameters.labelBlockLookupN5BlockSize,
			parameters.slicePositions,
			overwriteExisiting
		)
	}

	override val type: String
		get() = "label"

}

private val SUPPORTED_LABEL_TYPES = setOf(
	DataType.INT8, DataType.UINT8, DataType.INT16, DataType.UINT16,
	DataType.INT32, DataType.UINT32, DataType.INT64, DataType.UINT64,
)

@Throws(IOException::class)
private fun handleLabelDatasetInferType(
	sc: JavaSparkContext,
	info: DatasetInfo,
	blockSize: IntArray,
	scales: Array<IntArray>,
	downsamplingBlockSizes: Array<IntArray>,
	maxNumEntries: IntArray,
	reverse: Boolean,
	winnerTakesAll: Boolean,
	labelBlockLookupN5BlockSize: Int?,
	slicePositions: String?,
	overwriteExisiting: Boolean = false
) {
	/* the dataType is only validated here; the spark converters resolve the actual type at runtime */
	if (createReader(info.inputContainer)?.getDatasetAttributes(info.inputDataset)?.dataType !in SUPPORTED_LABEL_TYPES)
		throw IOException("Unable to infer data type from dataset `${info.inputDataset}' in container `${info.inputContainer}'")
	handleLabelDataset(
		sc,
		info,
		blockSize,
		scales,
		downsamplingBlockSizes,
		maxNumEntries,
		reverse,
		winnerTakesAll,
		labelBlockLookupN5BlockSize,
		slicePositions,
		overwriteExisiting
	)
}

@Throws(IOException::class, InvalidDataType::class, InvalidN5Container::class, InvalidDataset::class, InputSameAsOutput::class)
private fun handleLabelDataset(
	sc: JavaSparkContext,
	info: DatasetInfo,
	initialBlockSize: IntArray,
	scales: Array<IntArray>,
	downsampleBlockSizes: Array<IntArray>,
	maxNumEntriesArray: IntArray,
	reverse: Boolean,
	winnerTakesAll: Boolean,
	labelBlockLookupN5BlockSize: Int?,
	slicePositions: String?,
	overwriteExisting: Boolean
) {
	val writer = createWriter(info.outputFormat, info.outputContainer)
	writer.createGroup(info.outputGroup)

	val dataGroup = "${info.outputGroup}/data"
	writer.createGroup(dataGroup)
	writer.setAttribute(dataGroup, "multiScale", true)
	val originalResolutionOutputDataset = scaleGroup(info.outputGroup, 0)
	val uniqueLabelsGroup = "${info.outputGroup}/unique-labels"
	val labelBlockMappingGroupBasename = "label-to-block-mapping"
	val labelBlockMappingGroup = "${info.outputGroup}/$labelBlockMappingGroupBasename"
	val labelBlockMappingGroupDirectory = File(labelBlockMappingGroup).absolutePath

	if (winnerTakesAll) {
		/* input type is erased and resolved at runtime; output is uint64 for winner-takes-all */
		N5ConvertSpark.convert<Nothing, UnsignedLongType>(
			sc,
			{ createReader(info.inputContainer) },
			info.inputDataset,
			{ createWriter(info.outputFormat, info.outputContainer) },
			originalResolutionOutputDataset,
			Optional.of(initialBlockSize),
			Optional.of(ZstandardCompression()), // TODO pass compression as parameter
			Optional.empty(),
			Optional.empty(),
			overwriteExisting
		)

		for ((scaleNum, scale) in scales.withIndex()) {
			val newScaleDataset = scaleGroup(info.outputGroup, scaleNum + 1)

			N5LabelDownsamplerSpark.downsampleLabel<UnsignedLongType>(
				sc,
				{ createWriter(info.outputFormat, info.outputContainer) },
				scaleGroup(info.outputGroup, scaleNum),
				newScaleDataset,
				scale,
				downsampleBlockSizes[scaleNum]
			)
		}

		val maxId = ExtractUniqueLabelsPerBlock.extractUniqueLabels(
			sc,
			info.outputContainerUri,
			info.outputContainerUri,
			originalResolutionOutputDataset,
			Paths.get(uniqueLabelsGroup, "s0").toString()
		)
		LabelListDownsampler.addMultiScaleTag(writer, uniqueLabelsGroup)

		writer.setAttribute(info.outputGroup, "maxId", maxId)

		if (scales.isNotEmpty())
		// TODO refactor this to be nicer
		{
			LabelListDownsampler.donwsampleMultiscale(sc, info.outputContainerUri, uniqueLabelsGroup, scales, downsampleBlockSizes)
		}
	} else {
		// TODO pass compression and reverse array as parameters
		if (slicePositions != null) {
			/* nD input: convert from a lazy, disk-cached 3D slice instead of the source path */
			val dimensions = createReader(info.inputContainer)!!.getDatasetAttributes(info.inputDataset).dimensions
			val spec = parseSlicePositions(slicePositions, dimensions)
			val slicedSupplier = slicedInputImgSupplier(info.inputContainer, info.inputDataset, spec, initialBlockSize)
			ConvertToLabelMultisetType.convertToLabelMultisetType(
				sc,
				slicedSupplier,
				initialBlockSize,
				initialBlockSize,
				info.outputContainerUri,
				originalResolutionOutputDataset,
				ZstandardCompression()
			)
		} else {
			ConvertToLabelMultisetType.convertToLabelMultisetType<Nothing>(
				sc,
				info.inputContainer,
				info.inputDataset,
				initialBlockSize,
				info.outputContainerUri,
				originalResolutionOutputDataset,
				ZstandardCompression(),
				reverse
			)
		}


		writer.setAttribute(info.outputGroup, "maxId", writer.getAttribute(originalResolutionOutputDataset, "maxId", Long::class.java))

		ExtractUniqueLabelsPerBlock.extractUniqueLabels(
			sc,
			info.outputContainerUri,
			info.outputContainerUri,
			originalResolutionOutputDataset,
			"$uniqueLabelsGroup/s0"
		)
		LabelListDownsampler.addMultiScaleTag(writer, uniqueLabelsGroup)

		if (scales.isNotEmpty()) {
			// TODO pass compression as parameter
			SparkDownsampler.downsampleMultiscale(sc, info.outputContainerUri, dataGroup, scales, downsampleBlockSizes, maxNumEntriesArray, ZstandardCompression())
			LabelListDownsampler.donwsampleMultiscale(sc, info.outputContainerUri, uniqueLabelsGroup, scales, downsampleBlockSizes)
		}
	}

	if (labelBlockLookupN5BlockSize != null) {
		LabelToBlockMapping.createMappingWithMultiscaleCheckN5(
			sc,
			info.outputContainerUri,
			uniqueLabelsGroup,
			info.outputContainerUri,
			info.outputGroup,
			labelBlockMappingGroupBasename,
			labelBlockLookupN5BlockSize
		)

	} else {
		LabelToBlockMapping.createMappingWithMultiscaleCheck(sc, info.outputContainerUri, uniqueLabelsGroup, labelBlockMappingGroupDirectory)
	}
	/* write the group-level lookup metadata */
	writer.setAttribute(info.outputGroup, LABEL_BLOCK_LOOKUP_KEY, LabelBlockLookupFromN5Relative("$labelBlockMappingGroupBasename/s%d"))
}
