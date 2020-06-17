package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.Expression

enum class Mode {
    INTERPRETER, CLOSURE_COMPILER, BYTECODE_COMPILER
}

abstract class RowCallable {
    abstract operator fun invoke(row: Array<Any?>, acc: Array<Accumulator>): Any?
}

private class InterpretedRowCallable(val expression: Expression): RowCallable() {
    override fun invoke(row: Array<Any?>, acc: Array<Accumulator>): Any? {
        return expression.accept(Interpreter(row, acc))
    }

}

fun compileExpression(expression: Expression, mode: Mode): RowCallable {
    return when (mode) {
        Mode.INTERPRETER -> InterpretedRowCallable(expression)
        Mode.CLOSURE_COMPILER -> expression.accept(ClosureCompiler())
        Mode.BYTECODE_COMPILER -> compile(expression)
    }
}