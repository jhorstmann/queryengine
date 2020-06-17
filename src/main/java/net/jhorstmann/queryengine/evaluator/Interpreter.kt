package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.*
import net.jhorstmann.queryengine.ast.Function


class Interpreter(val row: Array<Any?>, val accumulators: Array<Accumulator>) : ExpressionVisitor<Any?> {

    override fun visitIdentifier(expr: IdentifierExpression): Any? {
        throw IllegalStateException("Identifier not expected during evaluation")
    }

    override fun visitNumericLiteral(expr: NumericLiteralExpression): Any? {
        return expr.value
    }

    override fun visitBooleanLiteral(expr: BooleanLiteralExpression): Any? {
        return expr.value
    }

    override fun visitStringLiteral(expr: StringLiteralExpression): Any? {
        return expr.value
    }

    override fun visitColumn(expr: ColumnExpression): Any? {
        return row[expr.index]
    }

    override fun visitFunction(expr: FunctionExpression): Any? {

        val ops = expr.operands

        // LOGIC functions might have lazy evaluation semantics
        // everything else can be evaluated up front to avoid some repeated code
        val args = when (expr.function.type) {
            FunctionType.LOGIC -> emptyList()
            else -> ops.map { it.accept(this) }
        }

        if (args.any { it == null }) {
            return null
        }

        return when (expr.function) {
            Function.IF -> {
                val cond = ops[0].accept(this) as Boolean?
                when {
                    cond == null -> null
                    cond -> ops[1].accept(this)
                    else -> ops[2].accept(this)
                }
            }
            Function.AND -> {
                val p = ops[0].accept(this) as Boolean?
                when {
                    p == null -> {
                        val q = ops[1].accept(this) as Boolean?
                        if (q == null) {
                            null
                        } else if (!q) {
                            false
                        } else {
                            null
                        }
                    }
                    p -> {
                        ops[1].accept(this) as Boolean?
                    }
                    else -> false
                }
            }
            Function.OR -> {
                val p = ops[0].accept(this) as Boolean?
                when {
                    p == null -> {
                        val q = ops[1].accept(this) as Boolean?
                        if (q == null) {
                            null
                        } else if (q) {
                            true
                        } else {
                            null
                        }
                    }
                    p -> true
                    else -> {
                        ops[1].accept(this) as Boolean?
                    }
                }
            }
            Function.NOT -> !(ops[0].accept(this) as Boolean)

            Function.UNARY_PLUS -> args[0] as Double
            Function.UNARY_MINUS -> -(args[0] as Double)
            Function.ADD -> args[0] as Double + args[1] as Double
            Function.SUB -> args[0] as Double - args[1] as Double
            Function.MUL -> args[0] as Double * args[1] as Double
            Function.DIV -> args[0] as Double / args[1] as Double
            Function.MOD -> args[0] as Double % args[1] as Double

            Function.CMP_EQ -> args[0] == args[1]
            Function.CMP_NE -> args[0] != args[1]
            Function.CMP_LT -> (args[0] as Comparable<Any>) < (args[1] as Comparable<Any>)
            Function.CMP_LE -> (args[0] as Comparable<Any>) <= (args[1] as Comparable<Any>)
            Function.CMP_GE -> (args[0] as Comparable<Any>) >= (args[1] as Comparable<Any>)
            Function.CMP_GT -> (args[0] as Comparable<Any>) > (args[1] as Comparable<Any>)
        }
    }

    override fun visitAggregationFunction(expr: AggregationFunctionExpression): Any? {
        val arg = expr.operands[0].accept(this)

        val idx = expr.accumulatorIndex
        val acc = accumulators[idx]
        if (arg != null) {
            acc.accumulate(arg)
        }
        return null
    }
}

