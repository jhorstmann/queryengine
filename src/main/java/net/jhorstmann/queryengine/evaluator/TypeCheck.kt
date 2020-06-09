package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.*
import net.jhorstmann.queryengine.ast.Function
import net.jhorstmann.queryengine.data.DataType
import java.lang.RuntimeException

class TypeCheckException(message: String) : RuntimeException(message)

fun typeCheck(expr: Expression): Expression {
    return expr.accept(TypeCheckVisitor)
}

fun typeCheck(plan: LogicalNode): LogicalNode {
    return when (plan) {
        is LogicalScanNode -> plan
        is LogicalFilterNode -> {
            val filter = plan.filter.accept(TypeCheckVisitor)
            val source = typeCheck(plan.source)
            LogicalFilterNode(source, filter)
        }
        is LogicalProjectionNode -> {
            val expressions = plan.expressions.map { it.accept(TypeCheckVisitor) }
            val source = typeCheck(plan.source)
            LogicalProjectionNode(source, expressions)
        }
        is LogicalAggregationNode -> {
            val aggregate = plan.aggregate.map { it.accept(TypeCheckVisitor) }
            val groupBy = plan.groupBy.map { it.accept(TypeCheckVisitor) }
            val source = typeCheck(plan.source)
            LogicalAggregationNode(source, groupBy, aggregate, plan.aggregateFunctions)
        }
    }
}

private object TypeCheckVisitor : DefaultExpressionVisitor() {

    override fun visitColumn(expr: ColumnExpression): Expression {
        // type was assigned in ResolveSchema
        return super.visitColumn(expr)
    }

    override fun visitFunction(expr: FunctionExpression): Expression {
        val operands = visitOperands(expr.operands)
        val function = expr.function

        when (function) {
            Function.UNARY_MINUS, Function.UNARY_PLUS,
            Function.ADD, Function.SUB, Function.MUL, Function.DIV, Function.MOD -> {
                if (operands[0].dataType != DataType.DOUBLE || operands[1].dataType != DataType.DOUBLE) {
                    throw invalidTypes(function, operands)
                } else {
                    return expr.with(operands = operands, dataType = DataType.DOUBLE)
                }
            }
            Function.NOT -> {
                if (operands[0].dataType != DataType.BOOLEAN) {
                    throw invalidTypes(function, operands)
                } else {
                    return expr.with(operands = operands, dataType = DataType.BOOLEAN)
                }
            }
            Function.CMP_EQ, Function.CMP_NE -> {
                if (operands[0].dataType != operands[1].dataType) {
                    throw invalidTypes(function, operands)
                } else {
                    return expr.with(operands = operands, dataType = DataType.BOOLEAN)
                }
            }
            Function.CMP_LT, Function.CMP_LE, Function.CMP_GE, Function.CMP_GT -> {
                if (operands[0].dataType != DataType.DOUBLE || operands[1].dataType != DataType.DOUBLE) {
                    throw invalidTypes(function, operands)
                } else {
                    return expr.with(operands = operands, dataType = DataType.BOOLEAN)
                }
            }
            Function.OR, Function.AND -> {
                if (operands[0].dataType != DataType.DOUBLE || operands[1].dataType != DataType.BOOLEAN) {
                    throw invalidTypes(function, operands)
                } else {
                    return expr.with(operands = operands, dataType = DataType.BOOLEAN)
                }
            }
            Function.IF -> {
                if (operands[0].dataType != DataType.BOOLEAN) {
                    throw invalidTypes(function, operands)
                } else if (operands.size > 2 && operands[1].dataType != operands[2].dataType) {
                    throw invalidTypes(function, operands)
                } else {
                    return expr.with(operands = operands, dataType = operands[1].dataType)
                }
            }
        }
    }

    override fun visitAggregationFunction(expr: AggregationFunctionExpression): Expression {
        val operands = visitOperands(expr.operands)
        val function = expr.function
        when (function) {
            AggregationFunction.ANY, AggregationFunction.ALL -> {
                if (operands[0].dataType != DataType.BOOLEAN) {
                    throw invalidTypes(function, operands)
                } else {
                    val dataType = DataType.BOOLEAN
                    return expr.with(operands = operands, dataType = dataType)
                }
            }
            AggregationFunction.MIN, AggregationFunction.MAX, AggregationFunction.SUM -> {
                // MIN/MAX could also be implemented for BOOLEANS
                if (operands[0].dataType != DataType.DOUBLE) {
                    throw invalidTypes(function, operands)
                } else {
                    return expr.with(operands = operands, dataType = DataType.DOUBLE)
                }
            }
            AggregationFunction.COUNT -> {
                return expr.with(operands = operands, dataType = DataType.DOUBLE)
            }

        }
    }

    private fun invalidTypes(function: Function, operands: List<Expression>): TypeCheckException {
        return TypeCheckException("Invalid operand types for [$function] ${operands.map { it.dataType }}")
    }

    private fun invalidTypes(function: AggregationFunction, operands: List<Expression>): TypeCheckException {
        return TypeCheckException("Invalid operand types for aggregation [$function] ${operands.map { it.dataType }}")
    }

}
