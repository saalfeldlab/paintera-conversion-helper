package org.janelia.saalfeldlab.conversion.to.paintera

import com.google.gson.JsonElement
import net.imglib2.type.NativeType
import net.imglib2.type.label.LabelMultisetType
import net.imglib2.type.numeric.IntegerType
import net.imglib2.type.numeric.integer.*
import org.apache.spark.api.java.JavaSparkContext
import org.janelia.saalfeldlab.conversion.DatasetInfo
import org.janelia.saalfeldlab.conversion.createReader
import org.janelia.saalfeldlab.conversion.createWriter
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
import org.janelia.saalfeldlab.n5.GzipCompression
import org.janelia.saalfeldlab.n5.spark.N5ConvertSpark
import org.janelia.saalfeldlab.n5.spark.downsample.N5LabelDownsamplerSpark
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
			overwriteExisiting
		)
	}

	override val type: String
		get() = "label"

}

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
	overwriteExisiting: Boolean = false
) {
	if (winnerTakesAll)
		when (createReader(info.inputContainer)?.getDatasetAttributes(info.inputDataset)?.dataType) {
			DataType.INT8 -> handleLabelDataset<ByteType, UnsignedLongType>(
				sc,
				info,
				blockSize,
				scales,
				downsamplingBlockSizes,
				maxNumEntries,
				reverse,
				winnerTakesAll,
				labelBlockLookupN5BlockSize,
				overwriteExisiting
			)

			DataType.UINT8 -> handleLabelDataset<UnsignedByteType, UnsignedLongType>(
				sc,
				info,
				blockSize,
				scales,
				downsamplingBlockSizes,
				maxNumEntries,
				reverse,
				winnerTakesAll,
				labelBlockLookupN5BlockSize,
				overwriteExisiting
			)

			DataType.INT16 -> handleLabelDataset<ShortType, UnsignedLongType>(
				sc,
				info,
				blockSize,
				scales,
				downsamplingBlockSizes,
				maxNumEntries,
				reverse,
				winnerTakesAll,
				labelBlockLookupN5BlockSize,
				overwriteExisiting
			)

			DataType.UINT16 -> handleLabelDataset<UnsignedShortType, UnsignedLongType>(
				sc,
				info,
				blockSize,
				scales,
				downsamplingBlockSizes,
				maxNumEntries,
				reverse,
				winnerTakesAll,
				labelBlockLookupN5BlockSize,
				overwriteExisiting
			)

			DataType.INT32 -> handleLabelDataset<IntType, UnsignedLongType>(
				sc,
				info,
				blockSize,
				scales,
				downsamplingBlockSizes,
				maxNumEntries,
				reverse,
				winnerTakesAll,
				labelBlockLookupN5BlockSize,
				overwriteExisiting
			)

			DataType.UINT32 -> handleLabelDataset<UnsignedIntType, UnsignedLongType>(
				sc,
				info,
				blockSize,
				scales,
				downsamplingBlockSizes,
				maxNumEntries,
				reverse,
				winnerTakesAll,
				labelBlockLookupN5BlockSize,
				overwriteExisiting
			)

			DataType.INT64 -> handleLabelDataset<LongType, UnsignedLongType>(
				sc,
				info,
				blockSize,
				scales,
				downsamplingBlockSizes,
				maxNumEntries,
				reverse,
				winnerTakesAll,
				labelBlockLookupN5BlockSize,
				overwriteExisiting
			)

			DataType.UINT64 -> handleLabelDataset<UnsignedLongType, UnsignedLongType>(
				sc,
				info,
				blockSize,
				scales,
				downsamplingBlockSizes,
				maxNumEntries,
				reverse,
				winnerTakesAll,
				labelBlockLookupN5BlockSize,
				overwriteExisiting
			)

			else -> throw IOException("Unable to infer data type from dataset `${info.inputDataset}' in container `${info.inputContainer}'")
		}
	else
		when (createReader(info.inputContainer)?.getDatasetAttributes(info.inputDataset)?.dataType) {
			DataType.INT8 -> handleLabelDataset<ByteType, LabelMultisetType>(
				sc,
				info,
				blockSize,
				scales,
				downsamplingBlockSizes,
				maxNumEntries,
				reverse,
				winnerTakesAll,
				labelBlockLookupN5BlockSize,
				overwriteExisiting
			)

			DataType.UINT8 -> handleLabelDataset<UnsignedByteType, LabelMultisetType>(
				sc,
				info,
				blockSize,
				scales,
				downsamplingBlockSizes,
				maxNumEntries,
				reverse,
				winnerTakesAll,
				labelBlockLookupN5BlockSize,
				overwriteExisiting
			)

			DataType.INT16 -> handleLabelDataset<ShortType, LabelMultisetType>(
				sc,
				info,
				blockSize,
				scales,
				downsamplingBlockSizes,
				maxNumEntries,
				reverse,
				winnerTakesAll,
				labelBlockLookupN5BlockSize,
				overwriteExisiting
			)

			DataType.UINT16 -> handleLabelDataset<UnsignedShortType, LabelMultisetType>(
				sc,
				info,
				blockSize,
				scales,
				downsamplingBlockSizes,
				maxNumEntries,
				reverse,
				winnerTakesAll,
				labelBlockLookupN5BlockSize,
				overwriteExisiting
			)

			DataType.INT32 -> handleLabelDataset<IntType, LabelMultisetType>(
				sc,
				info,
				blockSize,
				scales,
				downsamplingBlockSizes,
				maxNumEntries,
				reverse,
				winnerTakesAll,
				labelBlockLookupN5BlockSize,
				overwriteExisiting
			)

			DataType.UINT32 -> handleLabelDataset<UnsignedIntType, LabelMultisetType>(
				sc,
				info,
				blockSize,
				scales,
				downsamplingBlockSizes,
				maxNumEntries,
				reverse,
				winnerTakesAll,
				labelBlockLookupN5BlockSize,
				overwriteExisiting
			)

			DataType.INT64 -> handleLabelDataset<LongType, LabelMultisetType>(
				sc,
				info,
				blockSize,
				scales,
				downsamplingBlockSizes,
				maxNumEntries,
				reverse,
				winnerTakesAll,
				labelBlockLookupN5BlockSize,
				overwriteExisiting
			)

			DataType.UINT64 -> handleLabelDataset<UnsignedLongType, LabelMultisetType>(
				sc,
				info,
				blockSize,
				scales,
				downsamplingBlockSizes,
				maxNumEntries,
				reverse,
				winnerTakesAll,
				labelBlockLookupN5BlockSize,
				overwriteExisiting
			)

			else -> throw IOException("Unable to infer data type from dataset `${info.inputDataset}' in container `${info.inputContainer}'")
		}
}

@Throws(IOException::class, InvalidDataType::class, InvalidN5Container::class, InvalidDataset::class, InputSameAsOutput::class)
private fun <I, O> handleLabelDataset(
	sc: JavaSparkContext,
	info: DatasetInfo,
	initialBlockSize: IntArray,
	scales: Array<IntArray>,
	downsampleBlockSizes: Array<IntArray>,
	maxNumEntriesArray: IntArray,
	reverse: Boolean,
	winnerTakesAll: Boolean,
	labelBlockLookupN5BlockSize: Int?,
	overwriteExisting: Boolean
) where
		I : IntegerType<I>, I : NativeType<I>,
		O : IntegerType<O>, O : NativeType<O> {
	val writer = createWriter(info.outputContainer)
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
		N5ConvertSpark.convert<I, O>(
			sc,
			{ createReader(info.inputContainer) },
			info.inputDataset,
			{ createWriter(info.outputContainer) },
			originalResolutionOutputDataset,
			Optional.of(initialBlockSize),
			Optional.of(GzipCompression()), // TODO pass compression as parameter
			Optional.empty(),
			Optional.empty(),
			overwriteExisting
		)

		for ((scaleNum, scale) in scales.withIndex()) {
			val newScaleDataset = scaleGroup(info.outputGroup, scaleNum + 1)

			N5LabelDownsamplerSpark.downsampleLabel<O>(
				sc,
				{ createWriter(info.outputContainer) },
				scaleGroup(info.outputGroup, scaleNum),
				newScaleDataset,
				scale,
				downsampleBlockSizes[scaleNum]
			)
		}

		val maxId = ExtractUniqueLabelsPerBlock.extractUniqueLabels(
			sc,
			info.outputContainer,
			info.outputContainer,
			originalResolutionOutputDataset,
			Paths.get(uniqueLabelsGroup, "s0").toString()
		)
		LabelListDownsampler.addMultiScaleTag(writer, uniqueLabelsGroup)

		writer.setAttribute(info.outputGroup, "maxId", maxId)

		if (scales.isNotEmpty())
		// TODO refactor this to be nicer
		{
			LabelListDownsampler.donwsampleMultiscale(sc, info.outputContainer, uniqueLabelsGroup, scales, downsampleBlockSizes)
		}
	} else {
		// TODO pass compression and reverse array as parameters
		ConvertToLabelMultisetType.convertToLabelMultisetType<I>(
			sc,
			info.inputContainer,
			info.inputDataset,
			initialBlockSize,
			info.outputContainer,
			originalResolutionOutputDataset,
			GzipCompression(),
			reverse
		)


		writer.setAttribute(info.outputGroup, "maxId", writer.getAttribute(originalResolutionOutputDataset, "maxId", Long::class.java))

		ExtractUniqueLabelsPerBlock.extractUniqueLabels(
			sc,
			info.outputContainer,
			info.outputContainer,
			originalResolutionOutputDataset,
			"$uniqueLabelsGroup/s0"
		)
		LabelListDownsampler.addMultiScaleTag(writer, uniqueLabelsGroup)

		if (scales.isNotEmpty()) {
			// TODO pass compression as parameter
			SparkDownsampler.downsampleMultiscale(sc, info.outputContainer, dataGroup, scales, downsampleBlockSizes, maxNumEntriesArray, GzipCompression())
			LabelListDownsampler.donwsampleMultiscale(sc, info.outputContainer, uniqueLabelsGroup, scales, downsampleBlockSizes)
		}
	}

	if (labelBlockLookupN5BlockSize != null) {
		LabelToBlockMapping.createMappingWithMultiscaleCheckN5(
			sc,
			info.outputContainer,
			uniqueLabelsGroup,
			info.outputContainer,
			info.outputGroup,
			labelBlockMappingGroupBasename,
			labelBlockLookupN5BlockSize
		)

	} else {
		LabelToBlockMapping.createMappingWithMultiscaleCheck(sc, info.outputContainer, uniqueLabelsGroup, labelBlockMappingGroupDirectory)
	}
	writer.getAttribute(labelBlockMappingGroup, LABEL_BLOCK_LOOKUP_KEY, JsonElement::class.java)?.also { labelBlockLookup ->
		writer.setAttribute(info.outputGroup, LABEL_BLOCK_LOOKUP_KEY, labelBlockLookup)
	}
}
