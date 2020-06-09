package net.jhorstmann.queryengine.operator

import net.jhorstmann.queryengine.ast.AggregationFunction
import net.jhorstmann.queryengine.evaluator.*


class GlobalAggregationOperator(val source: Operator, val expressions: List<RowCallable>, val aggregateFunctions: List<AggregationFunction>) : Operator() {
    var result: Array<Any?>? = null

    override fun open() {
        val accumulators = initAccumulators()

        val source = this.source
        val expressions = this.expressions
        val len = expressions.size

        source.forEach { row ->
            for (i in 0 until len) {
                expressions[i].invoke(row, accumulators)
            }
        }

        result = Array(accumulators.size) { accumulators[it].finish() }
    }

    private fun initAccumulators(): Array<Accumulator> {
        return aggregateFunctions.map {
            when (it) {
                AggregationFunction.COUNT -> CountAccumulator()
                AggregationFunction.SUM -> SumAccumulator()
                AggregationFunction.MIN -> MinAccumulator()
                AggregationFunction.MAX -> MaxAccumulator()
                AggregationFunction.ALL -> TODO("ALL")
                AggregationFunction.ANY -> TODO("ANY")
            }
        }.toTypedArray()
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