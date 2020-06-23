package net.jhorstmann.queryengine.operator

import net.jhorstmann.queryengine.evaluator.*


class GlobalAggregationOperator(val source: Operator, val expressions: List<RowCallable>, val initAccumulators: () -> Array<Accumulator>) : Operator() {
    var result: Array<Any?>? = null

    override fun open() {
        val accumulators = initAccumulators()

        val source = this.source
        val expressions = this.expressions

        source.forEach { row ->
            expressions.forEach {
                it.invoke(row, accumulators)
            }
        }

        result = Array(accumulators.size) { accumulators[it].finish() }
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