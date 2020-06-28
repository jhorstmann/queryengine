package net.jhorstmann.queryengine.operator

import net.jhorstmann.queryengine.evaluator.RowCallable

class ProjectionOperator(val source: Operator, val expressions: List<RowCallable>) : Operator() {
    override fun open() {
        source.open()
    }

    override fun close() {
        source.close()
    }


    override fun next(): Array<Any?>? {
        val row = source.next() ?: return null

        return Array(expressions.size) { expressions[it](row) }
    }

}