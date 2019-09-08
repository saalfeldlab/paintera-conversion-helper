package org.janelia.saalfeldlab.conversion

import org.apache.spark.api.java.JavaSparkContext
import java.io.File
import java.lang.Exception

abstract class DatasetConverter(
        val info: DatasetInfo
) {

    open class InvalidInputContainer(val container: String, message: String = "Invalid input container `$container'") : ConversionException(message)
    open class InvalidInputDataset(val container: String, val dataset: String, message: String = "Invalid input dataset `$dataset' in `$container'") : ConversionException(message)
    class InputContainerDoesNotExist(container: String) : InvalidInputContainer(container, "Container `$container' does not exists.")
    class InputDatasetExistsButIsGroup(container: String, dataset: String) : InvalidInputDataset(container, dataset, "Input dataset `$dataset' is a group in container `$container'.")
    class InputDatasetDoesNotExist(container: String, dataset: String) : InvalidInputDataset(container, dataset, "Input dataset `$dataset' does not exist in container `$container'.")
    class IncompatibleNumDimensions(
            container: String,
            dataset: String,
            val numDimensions: Int,
            val legalNumDimensions: Set<Int>,
            val type: String) : InvalidInputDataset(
            container,
            dataset,
            "Number of dimensions `$numDimensions' invalid for dataset `$dataset' in container `$container'. Valid numbers of dimensions for type `$type': $legalNumDimensions")

    @Throws(InvalidInputContainer::class, InvalidInputDataset::class)
    fun ensureInput(): Boolean {
        if (!File(info.inputContainer).exists())
            throw InputContainerDoesNotExist(info.inputContainer)
        info.inputContainer.n5Reader().let { container ->
            if (!container.exists(info.inputDataset))
                throw InputDatasetDoesNotExist(info.inputContainer, info.inputDataset)
            if (!container.datasetExists(info.inputDataset))
                throw InputDatasetExistsButIsGroup(info.inputContainer, info.inputDataset)
            container.getDatasetAttributes(info.inputDataset).let { attrs ->
                if (!legalDimensions.contains(attrs.numDimensions))
                    throw IncompatibleNumDimensions(info.inputContainer, info.inputDataset, attrs.numDimensions, legalDimensions, type)
            }
        }
        return true
    }

    abstract fun convert(
            sc: JavaSparkContext,
            parameters: DatasetSpecificParameters,
            overwriteExisiting: Boolean)

    protected open val legalDimensions: Set<Int>
        get() = setOf(3)

    protected abstract val type: String

    companion object {
        operator fun get(info: DatasetInfo, type: String) = when(type.toLowerCase()) {
            "raw" -> DatasetConverterRaw(info)
            else -> null
        }
    }

}