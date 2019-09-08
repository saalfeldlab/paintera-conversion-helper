package org.janelia.saalfeldlab.conversion

import java.lang.Exception

open class ConversionException(message: String? = null, cause: Throwable? = null) : Exception(message, cause)

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

open class InvalidOutputContainer(val container: String, message: String = "Invalid output container `$container'") : ConversionException(message)
open class InvalidOutputDataset(val container: String, val dataset: String, message: String = "Invalid dataset `$dataset' in output container `$container'") : ConversionException(message)
class OutputContainerIsFile(container: String) : InvalidOutputContainer(container, "Expected N5 container (directory) but found file `$container'.")
class OutputDatasetExists(container: String, dataset: String) : InvalidOutputDataset(container, dataset, "Dataset `$dataset' already exists in `$container'.")