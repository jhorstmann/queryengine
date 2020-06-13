package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.*
import net.jhorstmann.queryengine.ast.Function
import net.jhorstmann.queryengine.data.DataType
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CompilerTest {

    @EnumSource(Mode::class)
    @ParameterizedTest
    fun `should load column`(mode: Mode) {
        val expr = ColumnExpression("foo", 0, DataType.STRING)

        val callable = compileExpression(expr, mode)

        val res = callable(arrayOf("foobar"), emptyArray())

        assertEquals("foobar", res)
    }

    @EnumSource(Mode::class)
    @ParameterizedTest
    fun `should add numeric literals`(mode: Mode) {
        val expr = FunctionExpression(Function.ADD, listOf(NumericLiteralExpression(1.0), NumericLiteralExpression(2.0)), DataType.DOUBLE)

        val callable = compileExpression(expr, mode)

        val res = callable(emptyArray(), emptyArray())

        assertEquals(3.0, res)
    }

    @EnumSource(Mode::class)
    @ParameterizedTest
    fun `should aggregate inputs`(mode: Mode) {
        val expr = AggregationFunctionExpression(AggregationFunction.SUM, listOf(ColumnExpression("foo", 0, DataType.DOUBLE)), DataType.DOUBLE, 0)

        val callable = compileExpression(expr, mode)

        val acc = arrayOf<Accumulator>(SumAccumulator())

        callable(arrayOf(1.0), acc)
        callable(arrayOf(2.0), acc)

        assertEquals(3.0, acc[0].finish())
    }

    @EnumSource(Mode::class)
    @ParameterizedTest
    fun `should handle null values`(mode: Mode) {
        val expr = FunctionExpression(Function.MUL, listOf(ColumnExpression("foo", 0, DataType.DOUBLE), ColumnExpression("bar", 1, DataType.DOUBLE)), DataType.DOUBLE)

        val callable = compileExpression(expr, mode)

        val res = callable(arrayOf(10.0, null), emptyArray())

        assertNull(res)
    }


    @EnumSource(Mode::class)
    @ParameterizedTest
    fun `should handle null in boolean and expressions`(mode: Mode) {
        val expr = FunctionExpression(Function.AND, listOf(ColumnExpression("p", 0, DataType.BOOLEAN), ColumnExpression("q", 1, DataType.BOOLEAN)), DataType.BOOLEAN)

        val truth = listOf(
                arrayOf<Any?>(true, true) to true,
                arrayOf<Any?>(true, false) to false,
                arrayOf<Any?>(true, null) to null,
                arrayOf<Any?>(false, true) to false,
                arrayOf<Any?>(false, false) to false,
                arrayOf<Any?>(false, null) to false,
                arrayOf<Any?>(null, true) to null,
                arrayOf<Any?>(null, false) to false,
                arrayOf<Any?>(null, null) to null
        )

        val callable = compileExpression(expr, mode)

        for ((row, expected) in truth) {
            val res = callable(row, emptyArray())

            assertEquals(expected, res, "Expected (${row[0]} AND ${row[1]}) == $expected")
        }
    }

    @EnumSource(Mode::class)
    @ParameterizedTest
    fun `should handle null in boolean or expressions`(mode: Mode) {
        val expr = FunctionExpression(Function.OR, listOf(ColumnExpression("p", 0, DataType.BOOLEAN), ColumnExpression("q", 1, DataType.BOOLEAN)), DataType.BOOLEAN)

        val truth = listOf(
                arrayOf<Any?>(true, true) to true,
                arrayOf<Any?>(true, false) to true,
                arrayOf<Any?>(true, null) to true,
                arrayOf<Any?>(false, true) to true,
                arrayOf<Any?>(false, false) to false,
                arrayOf<Any?>(false, null) to null,
                arrayOf<Any?>(null, true) to true,
                arrayOf<Any?>(null, false) to null,
                arrayOf<Any?>(null, null) to null
        )

        val callable = compileExpression(expr, mode)

        for ((row, expected) in truth) {
            val res = callable(row, emptyArray())

            assertEquals(expected, res, "Expected (${row[0]} OR ${row[1]}) == $expected")
        }
    }

    @EnumSource(Mode::class)
    @ParameterizedTest
    fun `should handle null in if expressions`(mode: Mode) {
        val expr = FunctionExpression(Function.IF, listOf(ColumnExpression("cond", 0, DataType.BOOLEAN), StringLiteralExpression("t"), StringLiteralExpression("f")), DataType.STRING)

        val truth = listOf(
                arrayOf<Any?>(true) to "t",
                arrayOf<Any?>(false) to "f",
                arrayOf<Any?>(null) to null
        )

        val callable = compileExpression(expr, mode)

        for ((row, expected) in truth) {
            val res = callable(row, emptyArray())

            assertEquals(expected, res, "Expected (IF ${row[0]} THEN t ELSE f) == $expected")
        }
    }

}