package org.janelia.saalfeldlab.conversion.to

import org.apache.spark.SparkConf

private fun Any.nameFromAny() = (this::class.java.enclosingClass ?: this::class.java).name

/**
 * Limits the parallelism to mitigate IO penalties at higher numbers.
 */
internal const val MAX_DEFAULT_PARALLELISM = 24

internal fun defaultSparkMaster(): String {
	val parallelism = Runtime.getRuntime().availableProcessors().coerceAtMost(MAX_DEFAULT_PARALLELISM)
	return "local[$parallelism]"
}

internal fun newSparkConf(appName: String, sparkMaster: String? = null): SparkConf {
	return SparkConf()
		.setAppName(appName)
		.setMaster(sparkMaster ?: System.getProperty("spark.master") ?: defaultSparkMaster())
}

internal fun Any.newSparkConf(sparkMaster: String? = null) = newSparkConf(nameFromAny(), sparkMaster)

