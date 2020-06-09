package net.jhorstmann.queryengine.operator

import net.jhorstmann.queryengine.evaluator.Accumulator
import net.jhorstmann.queryengine.evaluator.RowCallable

class ProjectionOperator(val source: Operator, val expressions: List<RowCallable>) : Operator() {
    companion object {
        private val emptyAccumulator: Array<Accumulator> = emptyArray()
    }

    override fun open() {
        source.open()
    }

    override fun close() {
        source.close()
    }


    override fun next(): Array<Any?>? {
        val row = source.next() ?: return null

        return Array(expressions.size) { expressions[it](row, emptyAccumulator) }
    }

}