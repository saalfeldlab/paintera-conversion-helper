package org.janelia.saalfeldlab.conversion

import com.google.gson.GsonBuilder
import org.janelia.saalfeldlab.n5.universe.N5Factory

private fun defaultGsonBuilder(): GsonBuilder = GsonBuilder().setPrettyPrinting().disableHtmlEscaping()
internal fun defaultN5Factory(): N5Factory = N5Factory().apply {
	zarrDimensionSeparator("/")
	gsonBuilder(defaultGsonBuilder())
}

private val N5_FACTORY = defaultN5Factory()
internal fun createReader(path: String) = N5_FACTORY.openReader(path)
internal fun createWriter(path: String) = N5_FACTORY.openWriter(path)