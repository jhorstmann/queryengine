package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.AggregationFunction

internal fun initAccumulators(aggregateFunctions: List<AggregationFunction>): Array<Accumulator> {
    return aggregateFunctions.map { initAccumulator(it) }.toTypedArray()
}

internal fun initAccumulator(it: AggregationFunction): Accumulator {
    return when (it) {
        AggregationFunction.COUNT -> CountAccumulator()
        AggregationFunction.SUM -> SumAccumulator()
        AggregationFunction.AVG -> AvgAccumulator()
        AggregationFunction.MIN -> MinAccumulator()
        AggregationFunction.MAX -> MaxAccumulator()
        AggregationFunction.ALL -> TODO("ALL")
        AggregationFunction.ANY -> TODO("ANY")
    }
}

sealed class Accumulator() {
    abstract fun accumulate(value: Any)
    abstract fun finish(): Any?
}

class CountAccumulator(): Accumulator() {
    private var count: Int = 0

    override fun accumulate(value: Any) {
        count++
    }

    override fun finish(): Any? {
        return count
    }
}

class SumAccumulator() : Accumulator() {
    private var count: Int = 0
    private var sum : Double = 0.0

    override fun accumulate(value: Any) {
        count++;
        sum += value as Double
    }

    override fun finish(): Any? {
        return if (count == 0) {
            null
        } else {
            sum
        }
    }
}

class MinAccumulator() : Accumulator() {
    private var min: Double = Double.POSITIVE_INFINITY
    private var count = 0

    override fun accumulate(value: Any) {
        count++
        min = Math.min(min, value as Double)
    }

    override fun finish(): Any? {
        return if (count == 0) {
            null
        } else {
            return min
        }
    }
}

class MaxAccumulator() : Accumulator() {
    private var max: Double = Double.NEGATIVE_INFINITY
    private var count = 0

    override fun accumulate(value: Any) {
        count++
        max = Math.max(max, value as Double)
    }

    override fun finish(): Any? {
        return if (count == 0) {
            null
        } else {
            max
        }
    }
}

class AvgAccumulator(): Accumulator() {
    private var sum: Double = 0.0
    private var count: Int = 0

    override fun accumulate(value: Any) {
        count++
        sum += value as Double
    }

    override fun finish(): Any? {
        return if (count == 0) {
            null
        } else {
            sum / count
        }
    }
}