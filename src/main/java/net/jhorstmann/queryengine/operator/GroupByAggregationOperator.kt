package net.jhorstmann.queryengine.operator

import net.jhorstmann.queryengine.evaluator.Accumulator
import net.jhorstmann.queryengine.evaluator.RowCallable
import java.lang.IllegalStateException
import kotlin.collections.HashMap

class GroupByAggregationOperator(val source: Operator, val groupBy: List<RowCallable>, val aggregates: List<RowCallable>, val initAccumulators: () -> Array<Accumulator>): Operator() {
    private class Key(val row: Array<Any?>) {
        override fun equals(other: Any?): Boolean {
            return row.contentEquals((other as Key).row)
        }

        override fun hashCode(): Int {
            return row.contentHashCode()
        }
    }


    private var map : HashMap<Key, Array<Accumulator>>? = null
    private var iter: Iterator<Map.Entry<Key, Array<Accumulator>>>? = null

    override fun open() {
        val map = LinkedHashMap<Key, Array<Accumulator>>()

        val source = this.source
        val groupBy = this.groupBy
        val aggregates = this.aggregates
        val init = this.initAccumulators

        source.forEach { row ->
            val key = Key(Array(groupBy.size) {
                groupBy[it].invoke(row, emptyArray())
            })

            val acc = map.computeIfAbsent(key) { init() }

            aggregates.forEach {
                it.invoke(row, acc)
            }
        }

        this.map = map
        this.iter = map.entries.iterator()
    }

    override fun close() {
        this.map = null
        this.iter = null
    }

    override fun next(): Array<Any?>? {
        val iter = this.iter ?: throw IllegalStateException("Operator not opened")
        if (iter.hasNext()) {
            val entry = iter.next()

            val key = entry.key.row
            val acc = entry.value
            val row = Array(key.size + acc.size) {
                if (it < key.size) {
                    key[it]
                } else {
                    acc[it-key.size].finish()
                }
            }
            return row
        } else {
            return null
        }

    }
}