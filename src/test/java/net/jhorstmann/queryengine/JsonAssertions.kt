package net.jhorstmann.queryengine

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.lang.AssertionError

private val objectMapper = ObjectMapper().also {
    it.registerModule(KotlinModule())
    it.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    it.enable(SerializationFeature.INDENT_OUTPUT)
}

fun assertJsonEquals(expected: Any?, actual: Any?) {
    val expectedJson = objectMapper.writeValueAsString(expected)
    val actualJson = objectMapper.writeValueAsString(actual)

    if (expectedJson != actualJson) {
        val expectedLines = expectedJson.lines().toTypedArray()
        val actualLines = actualJson.lines().toTypedArray()

        val diff = Diff.formatDiff(expectedLines, actualLines).joinToString("\n")

        throw AssertionError("Difference:\n$diff")
    }

}