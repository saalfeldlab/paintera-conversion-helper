package org.janelia.saalfeldlab.conversion

import org.janelia.saalfeldlab.conversion.to.MAX_DEFAULT_PARALLELISM
import org.janelia.saalfeldlab.conversion.to.newSparkConf
import kotlin.test.Test
import kotlin.test.assertEquals

class CommonTest {

	@Test
	fun `default spark master`() {
		val expected = "local[${Runtime.getRuntime().availableProcessors().coerceAtMost(MAX_DEFAULT_PARALLELISM)}]"
		assertEquals(expected, newSparkConf().get("spark.master"))
	}

	@Test
	fun `spark master from system property`() {
		var expected = "spark://foo:1234"
		System.setProperty("spark.master", expected)
		assertEquals(expected, newSparkConf().get("spark.master"))
		System.clearProperty("spark.master")
	}

	@Test
	fun `spark master from param`() {
		var expected = "spark://foo:1234"
		assertEquals(expected, newSparkConf(expected).get("spark.master"))
	}

	@Test
	fun `default spark app name`() {
		val expected = CommonTest::class.java.name
		assertEquals(expected, newSparkConf().get("spark.app.name"))
	}

	@Test
	fun `spark app name from param`() {
		val expected = "common-test"
		assertEquals(expected, newSparkConf(appName = expected).get("spark.app.name"))
	}
}