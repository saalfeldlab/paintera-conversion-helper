package org.janelia.saalfeldlab.conversion

import io.github.oshai.kotlinlogging.KotlinLogging
import org.janelia.saalfeldlab.conversion.to.paintera.*
import org.janelia.saalfeldlab.n5.DataType
import org.janelia.saalfeldlab.n5.DatasetAttributes
import org.janelia.saalfeldlab.n5.N5Reader
import org.janelia.saalfeldlab.n5.N5URI
import java.io.Serializable

data class DatasetInfo(
	val inputContainer: String,
	val inputDataset: String,
	val outputContainer: String,
	val outputGroup: String = inputDataset
) : Serializable {

	val type: String
		get() {
			val attrs = attributes
			return if (attrs.numDimensions == 4)
				CHANNEL_IDENTIFIER
			else when (createReader(inputContainer).getDatasetAttributes(inputDataset).dataType) {
				DataType.UINT64, DataType.INT64, DataType.UINT32 -> LABEL_IDENTIFIER
				else -> RAW_IDENTIFIER
			}
		}

	val attributes: DatasetAttributes
		get() = createReader(inputContainer).getDatasetAttributes(inputDataset)

	@Throws(InvalidInputContainer::class, InvalidInputDataset::class)
	fun ensureInput(): Boolean {
		val n5: N5Reader
		try {
			n5 = createReader(inputContainer)
		} catch( e : Exception) {
			LOG.error(e) {"Could not load input container $inputContainer"}
			throw InputContainerDoesNotExist(inputContainer)
		}

		n5.let { container ->
			if (!container.exists(inputDataset))
				throw InputDatasetDoesNotExist(inputContainer, inputDataset)
			if (!container.datasetExists(inputDataset))
				throw InputDatasetExistsButIsGroup(inputContainer, inputDataset)
			// TODO deal with illegal dimensionsalities
//            container.getDatasetAttributes(inputDataset).let { attrs ->
//                if (!legalDimensions.contains(attrs.numDimensions))
//                    throw IncompatibleNumDimensions(info.inputContainer, info.inputDataset, attrs.numDimensions, legalDimensions, type)
//            }
		}
		return true
	}

	@Throws(InvalidInputContainer::class, InvalidInputDataset::class)
	fun ensureOutput(existOk: Boolean): Boolean {
		/* Check if already exists */
		val alreadyExists = runCatching { createWriter(outputContainer).exists("/") }.getOrNull() == true

		if ((!existOk && alreadyExists) && !inputSameAsOutput() && createWriter(outputContainer).exists(outputGroup))
			throw OutputDatasetExists(outputContainer, outputGroup)
		return true
	}

	fun inputSameAsOutput(): Boolean {
		val outputDataset = scaleGroup(outputGroup, 0)
		return N5URI(inputContainer) == N5URI(outputContainer) && N5URI.normalizeGroupPath(inputDataset) == N5URI.normalizeGroupPath(outputDataset)
	}

	companion object {
		private val LOG = KotlinLogging.logger {  }
	}
}