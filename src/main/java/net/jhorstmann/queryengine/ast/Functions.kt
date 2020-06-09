package net.jhorstmann.queryengine.ast

enum class FunctionType {
    LOGIC, ARITHMETIC, COMPARISON, AGGREGATION
}

enum class Function(val type: FunctionType, val arity: Int) {
    NOT(FunctionType.LOGIC, 1), AND(FunctionType.LOGIC, 2), OR(FunctionType.LOGIC, 2),
    IF(FunctionType.LOGIC, 3),

    UNARY_MINUS(FunctionType.ARITHMETIC, 1), UNARY_PLUS(FunctionType.ARITHMETIC, 1),

    MUL(FunctionType.ARITHMETIC, 2), DIV(FunctionType.ARITHMETIC, 2), MOD(FunctionType.ARITHMETIC, 2),
    ADD(FunctionType.ARITHMETIC, 2), SUB(FunctionType.ARITHMETIC, 2),

    CMP_LT(FunctionType.COMPARISON, 2), CMP_LE(FunctionType.COMPARISON, 2),
    CMP_GE(FunctionType.COMPARISON, 2), CMP_GT(FunctionType.COMPARISON, 2),
    CMP_EQ(FunctionType.COMPARISON, 2), CMP_NE(FunctionType.COMPARISON, 2),


    //MIN(FunctionType.AGGREGATION, 1), MAX(FunctionType.AGGREGATION, 1),
    //SUM(FunctionType.AGGREGATION, 1), COUNT(FunctionType.AGGREGATION, 1),
    //ANY(FunctionType.AGGREGATION, 1), ALL(FunctionType.AGGREGATION, 1)
}

enum class LazyFunction {
    IF, AND, OR
}

enum class AggregationFunction {
    MIN, MAX, SUM, COUNT, ANY, ALL
}