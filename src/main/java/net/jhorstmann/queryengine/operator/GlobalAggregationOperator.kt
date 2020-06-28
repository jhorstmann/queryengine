package net.jhorstmann.queryengine.operator

import net.jhorstmann.queryengine.ast.AggregationFunction
import net.jhorstmann.queryengine.evaluator.*


class GlobalAggregationOperator(val source: Operator, private val aggregateFunctions: List<AggregationFunction>) : Operator() {
    var result: Array<Any?>? = null

    override fun open() {
        val acc = initAccumulators(aggregateFunctions)

        val source = this.source

        source.forEach { row ->
            acc.forEachIndexed { i, a ->
                val v = row[i]
                if (v != null) {
                    a.accumulate(v)
                }
            }
        }

        result = Array(acc.size) { acc[it].finish() }
    }


    override fun close() {
        this.result = null
    }

    override fun next(): Array<Any?>? {
        val res = this.result
        this.result = null
        return res
    }
}