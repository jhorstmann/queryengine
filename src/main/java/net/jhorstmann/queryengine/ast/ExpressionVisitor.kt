package net.jhorstmann.queryengine.ast

interface ExpressionVisitor<R> {
    fun visitIdentifier(expr: IdentifierExpression): R
    fun visitNumericLiteral(expr: NumericLiteralExpression): R
    fun visitBooleanLiteral(expr: BooleanLiteralExpression): R
    fun visitStringLiteral(expr: StringLiteralExpression): R

    fun visitFunction(expr: FunctionExpression): R
    fun visitAggregationFunction(expr: AggregationFunctionExpression): R

    fun visitColumn(expr: ColumnExpression): R
}