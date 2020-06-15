package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.assertJsonEquals
import net.jhorstmann.queryengine.data.DataType
import net.jhorstmann.queryengine.data.Field
import net.jhorstmann.queryengine.data.MemoryTable
import net.jhorstmann.queryengine.data.Schema
import net.jhorstmann.queryengine.query
import org.junit.jupiter.api.Test

class QueryTest {

    @Test
    fun `should group by multiple columns`() {
        val schema = Schema(listOf(Field("foo", DataType.STRING), Field("bar", DataType.STRING), Field("num", DataType.DOUBLE)))
        val actual = query("table", MemoryTable(schema, listOf(
                listOf("a", "A", 1.0),
                listOf("a", "B", 2.0),
                listOf("a", "B", 3.0),
                listOf("b", "B", 4.0),
                listOf("b", "B", null),
                listOf("c", null, null)
        )), "SELECT bar, SUM(num), foo FROM table")

        val expected = listOf<Array<Any?>>(
                arrayOf("A", 1.0, "a"),
                arrayOf("B", 5.0, "a"),
                arrayOf("B", 4.0, "b"),
                arrayOf(null, null, "c")
        )

        assertJsonEquals(expected, actual)
    }
}