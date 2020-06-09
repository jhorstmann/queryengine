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

    internal fun defineClass(name: String, bytes: ByteArray) : Class<*> {
        return defineClass(name, bytes, 0, bytes.size)
    }
}

private val accumulatorType = Type.getType(Accumulator::class.java)
private val objectType = Type.getType(Object::class.java)
private val doubleType = Type.getType(java.lang.Double::class.java)
private val booleanType = Type.getType(java.lang.Boolean::class.java)
private val rowCallableType = Type.getType(RowCallable::class.java)

class CommonValueClassWriter(flags: Int) : ClassWriter(flags) {
    override fun getCommonSuperClass(type1: String, type2: String): String {
        return objectType.descriptor
    }
}

private val counter = AtomicInteger()




fun compile(expression: Expression) : RowCallable {
    val name = "Compiled${RowCallable::class.java.simpleName}\$${counter.incrementAndGet()}"

    val cw = CommonValueClassWriter(ClassWriter.COMPUTE_FRAMES or ClassWriter.COMPUTE_MAXS)
    val cv : ClassVisitor = CheckClassAdapter(cw)

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
    call.visitMaxs(maxStack,3)
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

    override fun visitColumn(expr: ColumnExpression): Int = 1

    override fun visitFunction(expr: FunctionExpression): Int {
        return 2 + (expr.operands.mapIndexed { idx, op -> idx + op.accept(this) }.max() ?: 0)
    }

    override fun visitAggregationFunction(expr: AggregationFunctionExpression): Int {
        return 2 + expr.operands[0].accept(this)
    }

}

private fun GeneratorAdapter.pushNull() {
    visitInsn(Opcodes.ACONST_NULL);
}

private fun GeneratorAdapter.addDouble() {
    visitInsn(Opcodes.DADD);
}

private class Compiler(private val ga: GeneratorAdapter) : ExpressionVisitor<Unit> {

    private val methodStart = Label()
    private val methodEnd = Label()

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

    fun binaryExpression(ops: List<Expression>, primitiveType: Type, opcode: Int) {
        val n1 = Label()
        val n2 = Label()
        val r = Label()

        ops[0].accept(this)
        ga.dup()
        ga.ifNull(n1)

        ops[1].accept(this)
        ga.dup()
        ga.ifNull(n2)

        ga.unbox(primitiveType)
        ga.swap(objectType, primitiveType)
        ga.unbox(primitiveType)
        ga.swap(primitiveType, primitiveType)
        ga.visitInsn(opcode)
        ga.box(primitiveType)

        ga.goTo(r)

        ga.mark(n2)
        ga.pop()

        ga.mark(n1)
        ga.pop()
        ga.visitInsn(Opcodes.ACONST_NULL)
        ga.mark(r)

    }

    override fun visitFunction(expr: FunctionExpression) {
        val ops = expr.operands

        when (expr.function) {
            Function.ADD -> binaryExpression(ops, Type.DOUBLE_TYPE, Opcodes.DADD)
            else -> TODO("not implemented")
        }
    }

    override fun visitAggregationFunction(expr: AggregationFunctionExpression) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visitColumn(expr: ColumnExpression) {
        ga.loadArg(1)
        ga.push(expr.index)
        ga.arrayLoad(objectType)
        when (expr.dataType) {
            DataType.DOUBLE ->        ga.checkCast(Type.DOUBLE_TYPE)
            DataType.BOOLEAN -> ga.checkCast(Type.BOOLEAN_TYPE)
            DataType.STRING -> {}
        }
    }

}