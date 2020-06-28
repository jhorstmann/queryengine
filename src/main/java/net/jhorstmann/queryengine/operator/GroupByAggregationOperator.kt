package net.jhorstmann.queryengine.operator

import net.jhorstmann.queryengine.ast.AggregationFunction
import net.jhorstmann.queryengine.evaluator.Accumulator
import net.jhorstmann.queryengine.evaluator.initAccumulator

class GroupByAggregationOperator(val source: Operator, private val groupCount: Int, private val aggregateFunctions: List<AggregationFunction>): Operator() {
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
        val groupCount = this.groupCount
        val aggregateFunctions = this.aggregateFunctions

        source.forEach { row ->
            if (row.size != groupCount + aggregateFunctions.size) {
                throw ArrayIndexOutOfBoundsException()
            }

            val key = Key(Array(groupCount) { row[it] })

            val acc = map.computeIfAbsent(key) {
                Array(aggregateFunctions.size) { initAccumulator(aggregateFunctions[it]) }
            }

            acc.forEachIndexed { i, a ->
                val v = row[groupCount + i]
                if (v != null) {
                    a.accumulate(v)
                }
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