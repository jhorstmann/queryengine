package net.jhorstmann.queryengine.operator

import java.lang.IllegalStateException

class OrderByOperator(val source: Operator, val index: Int): Operator() {
    private var data : List<Array<Any?>>? = null
    private var iter : Iterator<Array<Any?>>? = null

    override fun open() {
        val data = source.mapTo(ArrayList()) { it }
        data.sortBy { row -> row[index] as Comparable<Any?>? }

        this.data = data
        this.iter = data.iterator()
    }

    override fun close() {
        this.iter = null
        this.data = null
    }

    override fun next(): Array<Any?>? {
        val iter = this.iter ?: throw IllegalStateException("Operator not opened")

        if (iter.hasNext()) {
            return iter.next()
        } else {
            return null
        }
    }
}