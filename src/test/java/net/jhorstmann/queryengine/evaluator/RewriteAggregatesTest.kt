package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.assertJsonEquals
import net.jhorstmann.queryengine.ast.*
import net.jhorstmann.queryengine.ast.Function
import net.jhorstmann.queryengine.data.DataType
import net.jhorstmann.queryengine.data.Field
import net.jhorstmann.queryengine.data.Schema
import org.junit.jupiter.api.Test

class RewriteAggregatesTest {
    @Test
    fun `should rewrite single aggregate`() {
        val scan = LogicalScanNode("table", Schema(listOf(Field("foo", DataType.DOUBLE))))
        val input = LogicalProjectionNode(
                scan,
                listOf(AggregationFunctionExpression(
                        AggregationFunction.SUM,
                        listOf(ColumnExpression("foo", 0, DataType.DOUBLE)),
                        DataType.DOUBLE)))

        val expected = LogicalProjectionNode(
                LogicalAggregationNode(
                        LogicalProjectionNode(scan, listOf(ColumnExpression("foo", 0, DataType.DOUBLE))),
                        0,
                        listOf(AggregationFunction.SUM)),
                listOf(ColumnExpression("SUM", 0, DataType.DOUBLE)))

        assertJsonEquals(expected, rewriteAggregates(input))
    }

    @Test
    fun `should rewrite multiple aggregates`() {
        val scan = LogicalScanNode("table", Schema(listOf(Field("foo", DataType.DOUBLE), Field("bar", DataType.DOUBLE))))
        val input = LogicalProjectionNode(
                scan,
                listOf(
                        AggregationFunctionExpression(
                                AggregationFunction.SUM,
                                listOf(ColumnExpression("foo", 0, DataType.DOUBLE)),
                                DataType.DOUBLE),
                        AggregationFunctionExpression(
                                AggregationFunction.COUNT,
                                listOf(ColumnExpression("bar", 1, DataType.DOUBLE)),
                                DataType.DOUBLE),
                        AggregationFunctionExpression(
                                AggregationFunction.AVG,
                                listOf(ColumnExpression("foo", 0, DataType.DOUBLE)),
                                DataType.DOUBLE)))

        val expected = LogicalProjectionNode(
                LogicalAggregationNode(
                        LogicalProjectionNode(scan,                         listOf(
                                ColumnExpression("foo", 0, DataType.DOUBLE),
                                ColumnExpression("bar", 1, DataType.DOUBLE),
                                ColumnExpression("foo", 0, DataType.DOUBLE))),
                        0,
                        listOf(AggregationFunction.SUM, AggregationFunction.COUNT, AggregationFunction.AVG)),
                listOf(
                        ColumnExpression("SUM", 0, DataType.DOUBLE),
                        ColumnExpression("COUNT", 1, DataType.DOUBLE),
                        ColumnExpression("AVG", 2, DataType.DOUBLE)))

        assertJsonEquals(expected, rewriteAggregates(input))
    }

    @Test
    fun `should rewrite expression containing multiple aggregates`() {
        val scan = LogicalScanNode("table", Schema(listOf(Field("foo", DataType.DOUBLE), Field("bar", DataType.DOUBLE))))
        val input = LogicalProjectionNode(
                scan,
                listOf(
                        FunctionExpression(
                                Function.DIV,
                                listOf(
                                        AggregationFunctionExpression(
                                                AggregationFunction.COUNT,
                                                listOf(ColumnExpression("bar", 1, DataType.DOUBLE)),
                                                DataType.DOUBLE),
                                        AggregationFunctionExpression(
                                                AggregationFunction.COUNT,
                                                listOf(ColumnExpression("foo", 0, DataType.DOUBLE)),
                                                DataType.DOUBLE)),
                                DataType.DOUBLE)))

        val expected = LogicalProjectionNode(
                LogicalAggregationNode(
                        LogicalProjectionNode(scan, listOf(ColumnExpression("bar", 1, DataType.DOUBLE), ColumnExpression("foo", 0, DataType.DOUBLE))),
                        0,
                        listOf(AggregationFunction.COUNT, AggregationFunction.COUNT)),
                listOf(
                        FunctionExpression(
                                Function.DIV,
                                listOf(
                                        ColumnExpression("COUNT", 0, DataType.DOUBLE),
                                        ColumnExpression("COUNT", 1, DataType.DOUBLE)),
                                DataType.DOUBLE)))

        assertJsonEquals (expected, rewriteAggregates(input))
    }

}