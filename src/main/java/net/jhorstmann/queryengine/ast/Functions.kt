package net.jhorstmann.queryengine.ast

enum class FunctionType {
    LOGIC, ARITHMETIC, COMPARISON
}

enum class Function(val type: FunctionType, val arity: Int) {
    AND(FunctionType.LOGIC, 2), OR(FunctionType.LOGIC, 2),

    IF(FunctionType.LOGIC, 3),

    NOT(FunctionType.LOGIC, 1),

    UNARY_MINUS(FunctionType.ARITHMETIC, 1), UNARY_PLUS(FunctionType.ARITHMETIC, 1),

    MUL(FunctionType.ARITHMETIC, 2), DIV(FunctionType.ARITHMETIC, 2), MOD(FunctionType.ARITHMETIC, 2),
    ADD(FunctionType.ARITHMETIC, 2), SUB(FunctionType.ARITHMETIC, 2),

    CMP_LT(FunctionType.COMPARISON, 2), CMP_LE(FunctionType.COMPARISON, 2),
    CMP_GE(FunctionType.COMPARISON, 2), CMP_GT(FunctionType.COMPARISON, 2),
    CMP_EQ(FunctionType.COMPARISON, 2), CMP_NE(FunctionType.COMPARISON, 2),
}

enum class AggregationFunction {
    MIN, MAX, SUM, COUNT, AVG, ANY, ALL
}