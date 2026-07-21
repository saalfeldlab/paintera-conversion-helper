package org.janelia.saalfeldlab.conversion

import com.google.gson.GsonBuilder
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookup
import org.janelia.saalfeldlab.labels.blocks.LabelBlockLookupAdapter
import org.janelia.saalfeldlab.n5.KeyValueAccess
import org.janelia.saalfeldlab.n5.N5Reader
import org.janelia.saalfeldlab.n5.N5Writer
import org.janelia.saalfeldlab.n5.universe.N5FactoryWithCache
import org.janelia.saalfeldlab.n5.universe.StorageFormat
import software.amazon.awssdk.http.apache.ApacheHttpClient
import java.net.URI
import java.time.Duration

private fun defaultGsonBuilder(): GsonBuilder = GsonBuilder()
	.setPrettyPrinting()
	.disableHtmlEscaping()
	.registerTypeHierarchyAdapter(LabelBlockLookup::class.java, LabelBlockLookupAdapter.getJsonAdapter())

private class SynchronizedN5FactoryWithCache : N5FactoryWithCache() {

	@Synchronized
	override fun openReader(storage: StorageFormat?, access: KeyValueAccess, location: URI): N5Reader =
		super.openReader(storage, access, location)

	@Synchronized
	override fun openWriter(storage: StorageFormat?, access: KeyValueAccess, location: URI): N5Writer =
		super.openWriter(storage, access, location)
}

internal fun defaultN5Factory(): N5FactoryWithCache = SynchronizedN5FactoryWithCache().apply {
	options { opts ->
		opts.zarr2 { it.dimensionSeparator("/") }
		opts.gsonBuilder(defaultGsonBuilder())
	}
	s3Configuration { builder ->
		builder.httpClientBuilder(
			ApacheHttpClient.builder()
				.connectionTimeout(Duration.ofMillis(50000))
				.socketTimeout(Duration.ofMillis(50000))
				.maxConnections(500)
		)
		builder.overrideConfiguration { override ->
			override.retryStrategy { retry -> retry.maxAttempts(11) }
		}
	}
}

private val N5_FACTORY = defaultN5Factory()
internal fun createReader(path: String) = N5_FACTORY.openReader(path)
internal fun createReader(format: StorageFormat?, path: URI) = N5_FACTORY.openReader(format, path)
internal fun createWriter(path: String) = N5_FACTORY.openWriter(path)
internal fun createWriter(format: StorageFormat?, path: URI) = N5_FACTORY.openWriter(format, path)
