package org.janelia.saalfeldlab.conversion

import com.amazonaws.ClientConfiguration
import com.google.gson.GsonBuilder
import org.janelia.saalfeldlab.n5.universe.N5Factory

private fun defaultGsonBuilder(): GsonBuilder = GsonBuilder().setPrettyPrinting().disableHtmlEscaping()
internal fun defaultN5Factory(): N5Factory = N5Factory().apply {
	zarrDimensionSeparator("/")
	val config = ClientConfiguration().apply {
		connectionTimeout = 50000
		socketTimeout = 50000
		maxConnections = 500
		maxErrorRetry = 10
	}
	s3ClientConfiguration(config)
	gsonBuilder(defaultGsonBuilder())
}

private val N5_FACTORY = defaultN5Factory()
internal fun createReader(path: String) = N5_FACTORY.openReader(path)
internal fun createWriter(path: String) = N5_FACTORY.openWriter(path)