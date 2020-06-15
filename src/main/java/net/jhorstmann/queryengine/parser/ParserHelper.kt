package net.jhorstmann.queryengine.parser

import net.jhorstmann.queryengine.ast.Expression
import net.jhorstmann.queryengine.ast.Query
import org.antlr.v4.runtime.*
import org.antlr.v4.runtime.misc.ParseCancellationException


class SyntaxException(message: String) : RuntimeException(message)

private class LocationAwareParseCancellationException(val line: Int, val charPositionInLine: Int, message: String?, cause: Throwable?)
    : ParseCancellationException(message, cause)

private object ThrowingErrorListener : BaseErrorListener() {

    override fun syntaxError(recognizer: Recognizer<*, *>?, offendingSymbol: Any?, line: Int, charPositionInLine: Int, msg: String?, e: RecognitionException?) {
        throw LocationAwareParseCancellationException(line, charPositionInLine, msg, e)
    }
}

private fun newParser(input: CharStream): QueryParser {
    val lexer = QueryLexer(input)
    lexer.removeErrorListeners()
    lexer.addErrorListener(ThrowingErrorListener)
    val parser = QueryParser(BufferedTokenStream(lexer))
    parser.removeErrorListeners()
    parser.addErrorListener(ThrowingErrorListener)
    return parser
}

internal fun unquoteSingle(it: String) = it.substring(1, it.length - 1).replace("''", "'")
internal fun unquoteDouble(it: String) = it.substring(1, it.length - 1).replace("\"\"", "\"")
internal fun unquote(id: QueryParser.IdentifierContext) : String {
    val unquoted = id.IDENTIFIER()?.text
    val quoted = id.QUOTED_IDENTIFIER()?.text
    return unquoted ?: unquoteDouble(quoted!!)
}


private fun newParser(input: String): QueryParser {
    return newParser(CharStreams.fromString(input))
}

fun parseExpression(query: String) : Expression {
    return newParser(query).singleExpression().accept(ExpressionAstBuilder)
}

fun parseQuery(query: String) : Query {
    val selectCtx = newParser(query).selectStatement()

    val from = unquote(selectCtx.from)
    val select = selectCtx.select.expression().map { it.accept(ExpressionAstBuilder) }
    val filter = selectCtx.where?.expr?.accept(ExpressionAstBuilder)
    val orderByColumn = selectCtx.orderBy?.column?.text?.toInt()

    return Query(select, from, filter, orderByColumn)
}
