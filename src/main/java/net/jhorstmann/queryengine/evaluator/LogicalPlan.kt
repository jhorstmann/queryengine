package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.AggregationFunction
import net.jhorstmann.queryengine.ast.Expression
import net.jhorstmann.queryengine.data.Schema

sealed class LogicalNode
data class LogicalScanNode(val table: String, val schema: Schema): LogicalNode()
data class LogicalFilterNode(val source: LogicalNode, val filter: Expression): LogicalNode()
data class LogicalAggregationNode(val source: LogicalNode, val groupBy: List<Expression>, val aggregate: List<Expression>, val aggregateFunctions: List<AggregationFunction>): LogicalNode()
data class LogicalProjectionNode(val source: LogicalNode, val expressions: List<Expression>): LogicalNode()
data class LogicalOrderByNode(val source: LogicalNode, val index: Int) : LogicalNode()

