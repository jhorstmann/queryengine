package net.jhorstmann.queryengine.ast

import net.jhorstmann.queryengine.data.DataType
import java.lang.IllegalStateException

sealed class Expression() {
    abstract val dataType: DataType
    abstract fun <R> accept(visitor: ExpressionVisitor<R>): R
}

data class IdentifierExpression(val name: String) : Expression() {
    override val dataType: DataType
        get() = throw IllegalStateException("Unresolved identifier")
    override fun <R> accept(visitor: ExpressionVisitor<R>): R = visitor.visitIdentifier(this)
}

data class NumericLiteralExpression(val value: Double) : Expression() {
    override val dataType: DataType
        get() = DataType.DOUBLE
    override fun <R> accept(visitor: ExpressionVisitor<R>): R = visitor.visitNumericLiteral(this)
}

data class BooleanLiteralExpression(val value: Boolean) : Expression() {
    override val dataType: DataType
        get() = DataType.BOOLEAN
    override fun <R> accept(visitor: ExpressionVisitor<R>): R = visitor.visitBooleanLiteral(this)
}

data class StringLiteralExpression(val value: String) : Expression() {
    override val dataType: DataType
        get() = DataType.STRING
    override fun <R> accept(visitor: ExpressionVisitor<R>): R = visitor.visitStringLiteral(this)
}


data class FunctionExpression(val function: Function, val operands: List<Expression>, val dataTypeNullable: DataType? = null) : Expression() {
    override val dataType: DataType
        get() = dataTypeNullable ?: throw IllegalStateException("Data type not initialized")

    constructor(function: Function, vararg operands: Expression) : this(function, operands.toList())

    fun with(operands: List<Expression>, dataType: DataType) = this.copy(operands = operands, dataTypeNullable = dataType)

    override fun <R> accept(visitor: ExpressionVisitor<R>): R = visitor.visitFunction(this)
}

data class AggregationFunctionExpression(val function: AggregationFunction, val operands: List<Expression>, val dataTypeNullable: DataType? = null, val resultIndex: Int = -1) : Expression() {
    override val dataType: DataType
        get() = dataTypeNullable ?: throw IllegalStateException("Data type not initialized")

    constructor(function: AggregationFunction, vararg operands: Expression) : this(function, operands.toList())

    fun with(operands: List<Expression>, dataType: DataType) = this.copy(operands = operands, dataTypeNullable = dataType)
    fun with(operands: List<Expression>, resultIndex: Int) = this.copy(operands = operands, resultIndex = resultIndex)

    override fun <R> accept(visitor: ExpressionVisitor<R>): R = visitor.visitAggregationFunction(this)
}


data class ColumnExpression(val name: String, val index: Int, override val dataType: DataType): Expression() {
    override fun <R> accept(visitor: ExpressionVisitor<R>): R = visitor.visitColumn(this)
}