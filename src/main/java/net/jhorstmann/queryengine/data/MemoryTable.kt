package net.jhorstmann.queryengine.data

import net.jhorstmann.queryengine.operator.MemorySourceOperator
import net.jhorstmann.queryengine.operator.Operator
import java.lang.IllegalArgumentException

class MemoryTable(override val schema: Schema, val values: List<List<Any?>>) : Table() {
    override fun getScanOperator(projectedSchema: Schema): Operator {
        val indices = projectedSchema.fields.map { field ->
            val idx = schema.fields.indexOfFirst { it.name == field.name }
            if (idx < 0) {
                throw IllegalArgumentException("Unknown field ${field.name}")
            }
            idx
        }.toIntArray()

        return MemorySourceOperator(indices, values)
    }
}