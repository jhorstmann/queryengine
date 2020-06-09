package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.Expression

enum class Mode {
    INTERPRETER, CLOSURE_COMPILER, BYTECODE_COMPILER
}

abstract class RowCallable {
    abstract operator fun invoke(row: Array<Any?>, accumulators: Array<Accumulator>): Any?
}

private class InterpretedExpression(val expression: Expression): RowCallable() {
    override fun invoke(row: Array<Any?>, accumulators: Array<Accumulator>): Any? {
        return expression.accept(Interpreter(row, accumulators))
    }

}

fun compileExpression(expression: Expression, mode: Mode): RowCallable {
    return when (mode) {
        Mode.INTERPRETER -> InterpretedExpression(expression)
        Mode.CLOSURE_COMPILER -> expression.accept(ClosureCompiler())
        else -> throw IllegalArgumentException("Mode $mode not yet implemented")
    }
}