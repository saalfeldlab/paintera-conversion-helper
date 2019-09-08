package org.janelia.saalfeldlab.conversion

import java.io.Serializable

data class DatasetInfo(
        val inputContainer: String,
        val inputDataset: String,
        val outputContainer: String,
        val outputGroup: String = inputDataset) : Serializable {
    init {
        inputContainer != outputContainer
    }
}