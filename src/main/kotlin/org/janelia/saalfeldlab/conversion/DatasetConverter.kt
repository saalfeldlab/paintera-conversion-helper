package org.janelia.saalfeldlab.conversion

import org.apache.spark.api.java.JavaSparkContext
import java.io.File
import java.lang.Exception

abstract class DatasetConverter(val info: DatasetInfo) {

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