package net.jhorstmann.queryengine.operator

import net.jhorstmann.queryengine.evaluator.RowCallable

class FilterOperator(val source: Operator, val expression: RowCallable) : Operator() {
    override fun open() {
        source.open()
    }

    override fun close() {
        source.close()
    }

    override fun next(): Array<Any?>? {
        while (true) {
            val row = source.next() ?: return null
            val res = expression(row, emptyArray())
            if (res != null && (res as Boolean)) {
                return row
            }
        }

    }
}