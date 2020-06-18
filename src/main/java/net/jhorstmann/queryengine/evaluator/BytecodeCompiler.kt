package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.*
import net.jhorstmann.queryengine.ast.Function
import net.jhorstmann.queryengine.data.DataType
import org.objectweb.asm.*
import org.objectweb.asm.commons.GeneratorAdapter
import org.objectweb.asm.util.CheckClassAdapter
import java.io.FileOutputStream
import java.util.concurrent.atomic.AtomicInteger


private object CustomClassLoader : ClassLoader(CustomClassLoader::class.java.classLoader) {

    internal fun defineClass(name: String, bytes: ByteArray): Class<*> {
        return defineClass(name, bytes, 0, bytes.size)
    }
}

private val accumulatorType = Type.getType(Accumulator::class.java)
private val objectType = Type.getType(Object::class.java)
private val doubleType = Type.getType(java.lang.Double::class.java)
private val booleanType = Type.getType(java.lang.Boolean::class.java)
private val stringType = Type.getType(java.lang.String::class.java)
private val rowCallableType = Type.getType(RowCallable::class.java)

class CommonValueClassWriter(flags: Int) : ClassWriter(flags) {
    override fun getCommonSuperClass(type1: String, type2: String): String {
        return objectType.descriptor
    }
}

private val counter = AtomicInteger()


fun compile(expression: Expression): RowCallable {
    val name = "Compiled${RowCallable::class.java.simpleName}\$${counter.incrementAndGet()}"

    val cw = CommonValueClassWriter(ClassWriter.COMPUTE_FRAMES or ClassWriter.COMPUTE_MAXS)
    val cv: ClassVisitor = CheckClassAdapter(cw)

    cv.visit(Opcodes.V1_8, Opcodes.ACC_PUBLIC, name, null, rowCallableType.internalName, emptyArray())

    val init = cv.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null)

    init.visitCode()
    init.visitVarInsn(Opcodes.ALOAD, 0)
    init.visitMethodInsn(Opcodes.INVOKESPECIAL, rowCallableType.internalName, "<init>", "()V", false)

    init.visitInsn(Opcodes.RETURN)
    init.visitMaxs(1, 1)
    init.visitEnd()

    val invokeDesc = "([${objectType.descriptor}[${accumulatorType.descriptor})${objectType.descriptor}"
    val call = cv.visitMethod(Opcodes.ACC_PUBLIC, "invoke", invokeDesc, null, emptyArray())

    call.visitCode()

    val ga = GeneratorAdapter(call, Opcodes.ACC_PUBLIC, "invoke", invokeDesc)
    val compiler = Compiler(ga)
    expression.accept(compiler)
    ga.returnValue()

    val maxStack = expression.accept(MaxStackVisitor)
    call.visitMaxs(maxStack, 3)
    call.visitEnd()

    val bytes = cw.toByteArray()

    FileOutputStream("target/classes/$name.class").use {
        it.write(bytes)
    }

    val compiledExpressionClass = CustomClassLoader.defineClass(name, bytes)

    @Suppress("UNCHECKED_CAST")
    return compiledExpressionClass.getDeclaredConstructor().newInstance() as RowCallable
}

object MaxStackVisitor : ExpressionVisitor<Int> {
    override fun visitIdentifier(expr: IdentifierExpression): Int = 0

    override fun visitNumericLiteral(expr: NumericLiteralExpression): Int = 2

    override fun visitBooleanLiteral(expr: BooleanLiteralExpression): Int = 1

    override fun visitStringLiteral(expr: StringLiteralExpression): Int = 1

    override fun visitColumn(expr: ColumnExpression): Int = 2

    override fun visitFunction(expr: FunctionExpression): Int {
        return 2 + (expr.operands.mapIndexed { idx, op -> idx + op.accept(this) }.max() ?: 0)
    }

    override fun visitAggregationFunction(expr: AggregationFunctionExpression): Int {
        return 3 + expr.operands[0].accept(this)
    }

}

private fun GeneratorAdapter.pushNull() {
    visitInsn(Opcodes.ACONST_NULL);
}

private fun GeneratorAdapter.pop(type: Type) {
    if (type == Type.DOUBLE_TYPE || type == Type.LONG_TYPE) {
        pop2()
    } else {
        pop()
    }
}

private class Compiler(private val ga: GeneratorAdapter) : ExpressionVisitor<Unit> {

    override fun visitIdentifier(expr: IdentifierExpression) {
    }

    override fun visitNumericLiteral(expr: NumericLiteralExpression) {
        ga.push(expr.value)
        ga.box(Type.DOUBLE_TYPE)
    }

    override fun visitBooleanLiteral(expr: BooleanLiteralExpression) {
        ga.push(expr.value)
        ga.box(Type.BOOLEAN_TYPE)
    }

    override fun visitStringLiteral(expr: StringLiteralExpression) {
        ga.push(expr.value)
    }

    fun binaryExpression(ops: List<Expression>, primitiveType: Type, block: () -> Unit) {
        val ifnull1 = Label()
        val ifnull2 = Label()
        val end = Label()

        ops[0].accept(this)
        ga.dup()
        ga.ifNull(ifnull1)
        ga.unbox(primitiveType)

        ops[1].accept(this)
        ga.dup()
        ga.ifNull(ifnull2)
        ga.unbox(primitiveType)

        block()
        ga.goTo(end)

        ga.mark(ifnull2)
        ga.pop()
        ga.pop(primitiveType)
        ga.pushNull()
        ga.goTo(end)

        ga.mark(ifnull1)
        ga.pop()
        ga.pushNull()

        ga.mark(end)
    }

    fun unaryExpression(op: Expression, primitiveType: Type, block: () -> Unit) {
        val ifnull = Label()
        val end = Label()

        op.accept(this)
        ga.dup()
        ga.ifNull(ifnull)
        ga.unbox(primitiveType)

        block()
        ga.goTo(end)

        ga.mark(ifnull)
        ga.pop()
        ga.pushNull()

        ga.mark(end)
    }

    fun binaryArithmeticExpression(ops: List<Expression>, opcode: Int) {
        binaryExpression(ops, Type.DOUBLE_TYPE) {
            ga.visitInsn(opcode)
            ga.box(Type.DOUBLE_TYPE)
        }
    }

    fun binaryComparisonExpression(ops: List<Expression>, cmpType: Int) {
        val dataType = ops[0].dataType
        val type = when (dataType) {
            DataType.DOUBLE -> Type.DOUBLE_TYPE
            DataType.BOOLEAN -> Type.BOOLEAN_TYPE
            DataType.STRING -> stringType
        }

        binaryExpression(ops, type) {
            val eq = Label()
            val end = Label()

            when (dataType) {
                DataType.DOUBLE -> {
                    ga.visitMethodInsn(Opcodes.INVOKESTATIC, doubleType.internalName, "compare", "(DD)I", false)
                }
                DataType.STRING -> {
                    ga.visitMethodInsn(Opcodes.INVOKEVIRTUAL, stringType.internalName, "compareTo", "(${stringType.descriptor})I", false)
                }
                DataType.BOOLEAN -> {
                    ga.visitMethodInsn(Opcodes.INVOKESTATIC, booleanType.internalName, "compare", "(ZZ)I", false)
                }
            }

            ga.push(0)
            ga.ifICmp(cmpType, eq)
            ga.push(false)
            ga.box(Type.BOOLEAN_TYPE)
            ga.visitJumpInsn(Opcodes.GOTO, end)

            ga.mark(eq)
            ga.push(true)
            ga.box(Type.BOOLEAN_TYPE)

            ga.mark(end)
        }
    }

    override fun visitFunction(expr: FunctionExpression) {
        val ops = expr.operands

        when (expr.function) {
            Function.ADD -> binaryArithmeticExpression(ops, Opcodes.DADD)
            Function.SUB -> binaryArithmeticExpression(ops, Opcodes.DSUB)
            Function.MUL -> binaryArithmeticExpression(ops, Opcodes.DMUL)
            Function.DIV -> binaryArithmeticExpression(ops, Opcodes.DDIV)
            Function.MOD -> binaryArithmeticExpression(ops, Opcodes.DREM)

            Function.CMP_EQ -> binaryComparisonExpression(ops, GeneratorAdapter.EQ)
            Function.CMP_NE -> binaryComparisonExpression(ops, GeneratorAdapter.NE)
            Function.CMP_LT -> binaryComparisonExpression(ops, GeneratorAdapter.LT)
            Function.CMP_LE -> binaryComparisonExpression(ops, GeneratorAdapter.LE)
            Function.CMP_GE -> binaryComparisonExpression(ops, GeneratorAdapter.GE)
            Function.CMP_GT -> binaryComparisonExpression(ops, GeneratorAdapter.GT)

            Function.UNARY_PLUS -> ga.checkCast(doubleType)
            Function.UNARY_MINUS -> unaryExpression(ops[0], Type.DOUBLE_TYPE) {
                ga.visitInsn(Opcodes.DNEG)
                ga.box(Type.DOUBLE_TYPE)
            }
            Function.NOT -> unaryExpression(ops[0], Type.BOOLEAN_TYPE) {
                ga.push(1)
                ga.swap()
                ga.visitInsn(Opcodes.ISUB)
                ga.box(Type.BOOLEAN_TYPE)
            }
            Function.IF -> {
                val ifnull = Label()
                val iftrue = Label()
                val iffalse = Label()
                val end = Label()

                ops[0].accept(this)
                ga.dup()
                ga.ifNull(ifnull)
                ga.unbox(Type.BOOLEAN_TYPE)
                ga.push(1)

                ga.ifCmp(Type.INT_TYPE, GeneratorAdapter.EQ, iftrue)
                ga.mark(iffalse)
                ops[2].accept(this)
                ga.goTo(end)

                ga.mark(iftrue)
                ops[1].accept(this)
                ga.goTo(end)

                ga.mark(ifnull)
                ga.pop()
                ga.pushNull()

                ga.mark(end)
            }
            Function.AND -> {
                val nullAnd = Label()
                val trueAnd = Label()
                val falseAnd = Label()
                val resultTrue = Label()
                val resultFalse = Label()
                val resultNull = Label()
                val resultNullPop = Label()
                val end = Label()

                ops[0].accept(this)
                ga.dup()
                ga.ifNull(nullAnd)

                ga.unbox(Type.BOOLEAN_TYPE)
                ga.push(true)
                ga.ifCmp(Type.INT_TYPE, GeneratorAdapter.EQ, trueAnd)
                ga.goTo(falseAnd)

                ga.mark(trueAnd)
                ops[1].accept(this)
                ga.dup()
                ga.ifNull(resultNullPop)

                ga.unbox(Type.BOOLEAN_TYPE)
                ga.push(true)
                ga.ifCmp(Type.INT_TYPE, GeneratorAdapter.EQ, resultTrue)
                ga.goTo(resultFalse)

                ga.mark(nullAnd)
                ga.pop()
                ops[1].accept(this)
                ga.dup()
                ga.ifNull(resultNullPop)

                ga.unbox(Type.BOOLEAN_TYPE)
                ga.push(true)
                ga.ifCmp(Type.INT_TYPE, GeneratorAdapter.EQ, resultNull)
                ga.goTo(resultFalse)

                ga.mark(falseAnd)
                ga.goTo(resultFalse)

                ga.mark(resultTrue)
                ga.push(true)
                ga.box(Type.BOOLEAN_TYPE)
                ga.goTo(end)

                ga.mark(resultFalse)
                ga.push(0)
                ga.box(Type.BOOLEAN_TYPE)
                ga.goTo(end)

                ga.mark(resultNullPop)
                ga.pop()
                ga.pushNull()
                ga.goTo(end)

                ga.mark(resultNull)
                ga.pushNull()
                ga.goTo(end)

                ga.mark(end)
            }
            Function.OR -> {
                val nullOr = Label()
                val falseOr = Label()
                val trueOr = Label()
                val resultTrue = Label()
                val resultFalse = Label()
                val resultNull = Label()
                val resultNullPop = Label()
                val end = Label()

                ops[0].accept(this)
                ga.dup()
                ga.ifNull(nullOr)

                ga.unbox(Type.BOOLEAN_TYPE)
                ga.push(true)
                ga.ifCmp(Type.INT_TYPE, GeneratorAdapter.EQ, trueOr)
                ga.goTo(falseOr)

                ga.mark(trueOr)
                ga.goTo(resultTrue)

                ga.mark(falseOr)
                ops[1].accept(this)
                ga.dup()
                ga.ifNull(resultNullPop)

                ga.unbox(Type.BOOLEAN_TYPE)
                ga.push(true)
                ga.ifCmp(Type.INT_TYPE, GeneratorAdapter.EQ, resultTrue)
                ga.goTo(resultFalse)

                ga.mark(nullOr)
                ga.pop()
                ops[1].accept(this)
                ga.dup()
                ga.ifNull(resultNullPop)

                ga.unbox(Type.BOOLEAN_TYPE)
                ga.push(true)
                ga.ifCmp(Type.INT_TYPE, GeneratorAdapter.EQ, resultTrue)
                ga.goTo(resultNull)

                ga.mark(resultTrue)
                ga.push(true)
                ga.box(Type.BOOLEAN_TYPE)
                ga.goTo(end)

                ga.mark(resultFalse)
                ga.push(0)
                ga.box(Type.BOOLEAN_TYPE)
                ga.goTo(end)

                ga.mark(resultNullPop)
                ga.pop()
                ga.pushNull()
                ga.goTo(end)

                ga.mark(resultNull)
                ga.pushNull()
                ga.goTo(end)

                ga.mark(end)
            }
        }
    }

    override fun visitAggregationFunction(expr: AggregationFunctionExpression) {
        val ifnull = Label()
        val end = Label()
        expr.operands[0].accept(this)
        ga.dup()

        ga.ifNull(ifnull)

        ga.loadArg(1)
        ga.push(expr.accumulatorIndex)
        ga.arrayLoad(accumulatorType)
        ga.swap()
        ga.visitMethodInsn(Opcodes.INVOKEVIRTUAL, accumulatorType.internalName, "accumulate", "(${objectType.descriptor})V", false)
        ga.goTo(end)

        ga.mark(ifnull)
        ga.pop()


        ga.mark(end)
        ga.pushNull()
    }

    override fun visitColumn(expr: ColumnExpression) {
        ga.loadArg(0)
        ga.push(expr.index)
        ga.arrayLoad(objectType)
        when (expr.dataType) {
            DataType.DOUBLE -> ga.checkCast(doubleType)
            DataType.BOOLEAN -> ga.checkCast(booleanType)
            DataType.STRING -> ga.checkCast(stringType)
        }
    }

}