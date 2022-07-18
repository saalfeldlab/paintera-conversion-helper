package org.janelia.saalfeldlab.paintera.conversion

open class ConversionException(val exitCode: Int, message: String? = null, cause: Throwable? = null) : Exception(message, cause) {
	constructor(message: String? = null, cause: Throwable? = null) : this(255, message, cause)
}

open class InvalidInputContainer(val container: String, message: String = "Invalid input container `$container'") :
	ConversionException(exitCodes.INVALID_INPUT_CONTAINER, message)

open class InvalidInputDataset(val container: String, val dataset: String, message: String = "Invalid input dataset `$dataset' in `$container'") :
	ConversionException(exitCodes.INVALID_INPUT_DATASET, message)

class InputContainerDoesNotExist(container: String) : InvalidInputContainer(container, "Container `$container' does not exists.")
class InputDatasetExistsButIsGroup(container: String, dataset: String) :
	InvalidInputDataset(container, dataset, "Input dataset `$dataset' is a group in container `$container'.")

class InputDatasetDoesNotExist(container: String, dataset: String) :
	InvalidInputDataset(container, dataset, "Input dataset `$dataset' does not exist in container `$container'.")

class IncompatibleNumDimensions(
	container: String,
	dataset: String,
	val numDimensions: Int,
	val legalNumDimensions: Set<Int>,
	val type: String
) : InvalidInputDataset(
	container,
	dataset,
	"Number of dimensions `$numDimensions' invalid for dataset `$dataset' in container `$container'. Valid numbers of dimensions for type `$type': $legalNumDimensions"
)

open class InvalidOutputContainer(val container: String, message: String = "Invalid output container `$container'") :
	ConversionException(exitCodes.INVALID_OUTPUT_CONTAINER, message)

open class InvalidOutputDataset(val container: String, val dataset: String, message: String = "Invalid dataset `$dataset' in output container `$container'") :
	ConversionException(exitCodes.INVALID_OUTPUT_DATASET, message)

class OutputContainerIsFile(container: String) : InvalidOutputContainer(container, "Expected N5 container (directory) but found file `$container'.")
class OutputDatasetExists(container: String, dataset: String) : InvalidOutputDataset(container, dataset, "Dataset `$dataset' already exists in `$container'.")

class InvalidBlockSize(val blockSize: IntArray, message: String) : ConversionException(exitCodes.INVALID_BLOCK_SIZE, message)

class NoSparkMasterSpecified(sparkMasterFlag: String? = null) : ConversionException(
	exitCodes.NO_SPARK_MASTER,
	"No spark master specified. Use the `-Dspark.master=<master>' system property${sparkMasterFlag?.let { " or the `$sparkMasterFlag' option" } ?: ""}.")

object exitCodes {

	const val INVALID_INPUT_CONTAINER: Int = 1

	const val INVALID_INPUT_DATASET: Int = 2

	const val INVALID_OUTPUT_CONTAINER: Int = 3

	const val INVALID_OUTPUT_DATASET: Int = 4

	const val INVALID_BLOCK_SIZE: Int = 5

	const val NO_SPARK_MASTER: Int = 6

}