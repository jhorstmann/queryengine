package net.jhorstmann.queryengine.data

import net.jhorstmann.queryengine.operator.MemorySourceOperator
import net.jhorstmann.queryengine.operator.Operator
import java.lang.IllegalArgumentException

class MemoryTable(override val schema: Schema, val values: List<List<Any?>>) : Table() {
    override fun getScanOperator(projection: List<String>): Operator {
        val indices = projection.map { name ->
            val idx = schema.fields.indexOfFirst { it.name == name }
            if (idx < 0) {
                throw IllegalArgumentException("Unknown field $name")
            }
            idx
        }.toIntArray()

        return MemorySourceOperator(indices, values)
    }
}