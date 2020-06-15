package net.jhorstmann.queryengine.parser

import net.jhorstmann.queryengine.ast.*
import net.jhorstmann.queryengine.ast.Function
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ParserTest {
    @Test
    fun `should parse identifier`() {
        assertEquals(IdentifierExpression("foo"), parseExpression("foo"))
    }

    @Test
    fun `should parse numeric literal`() {
        assertEquals(NumericLiteralExpression(1.5), parseExpression("1.5"))
    }

    @Test
    fun `should parse unary expression`() {
        assertEquals(FunctionExpression(Function.UNARY_MINUS, IdentifierExpression("foo")), parseExpression("-foo"))
    }

    @Test
    fun `should parse if expression`() {
        assertEquals(FunctionExpression(Function.IF, IdentifierExpression("foo"), BooleanLiteralExpression(true), BooleanLiteralExpression(false)),
                parseExpression("IF foo THEN true ELSE FALSE END"))
    }

    @Test
    fun `should parse function expression`() {
        assertEquals(AggregationFunctionExpression(AggregationFunction.SUM, IdentifierExpression("foo")),
                parseExpression("SUM(foo)"))
    }

    @Test
    fun `should parse complete query`() {
        val actual = parseQuery("SELECT foo, bar FROM table WHERE baz ORDER BY 2")

        val expected = Query(
                listOf(
                        IdentifierExpression("foo"),
                        IdentifierExpression("bar")),
                "table",
                IdentifierExpression("baz"), 2)

        assertEquals(expected, actual)
    }

}