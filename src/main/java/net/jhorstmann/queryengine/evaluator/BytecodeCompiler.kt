package net.jhorstmann.queryengine.evaluator

import net.jhorstmann.queryengine.ast.*
import org.objectweb.asm.*
import org.objectweb.asm.util.CheckClassAdapter
import java.io.FileOutputStream
import java.util.concurrent.atomic.AtomicInteger


private object CustomClassLoader : ClassLoader(CustomClassLoader::class.java.classLoader) {

    internal fun defineClass(name: String, bytes: ByteArray) : Class<*> {
        return defineClass(name, bytes, 0, bytes.size)
    }
}

private val objectDesc = Type.getDescriptor(Object::class.java)
private val accumulatorDesc = Type.getDescriptor(Accumulator::class.java)

class CommonValueClassWriter(flags: Int) : ClassWriter(flags) {
    override fun getCommonSuperClass(type1: String, type2: String): String {
        return objectDesc
    }
}

private val counter = AtomicInteger()




fun compile(expression: Expression) : RowCallable {
    val name = "Compiled${RowCallable::class.java.simpleName}\$${counter.incrementAndGet()}"

    val cw = CommonValueClassWriter(ClassWriter.COMPUTE_FRAMES or ClassWriter.COMPUTE_MAXS)
    val cv : ClassVisitor = CheckClassAdapter(cw)

    val init = cv.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null)

    init.visitCode()
    init.visitVarInsn(Opcodes.ALOAD, 0)
    init.visitMethodInsn(Opcodes.INVOKESPECIAL, "Ljava/lang/Object;", "<init>", "()V", false)

    init.visitInsn(Opcodes.RETURN)
    init.visitMaxs(1, 1)
    init.visitEnd()

    val call = cv.visitMethod(Opcodes.ACC_PUBLIC, "invoke", "([$objectDesc[$accumulatorDesc)$objectDesc", null, emptyArray())

    call.visitCode()

    val compiler = Compiler(call)

    expression.accept(compiler)

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

    override fun visitNumericLiteral(expr: NumericLiteralExpression): Int = 1

    override fun visitBooleanLiteral(expr: BooleanLiteralExpression): Int = 1

    override fun visitStringLiteral(expr: StringLiteralExpression): Int = 1

    override fun visitColumn(expr: ColumnExpression): Int = 1

    override fun visitFunction(expr: FunctionExpression): Int {
        return 1 + (expr.operands.mapIndexed { idx, op -> idx + op.accept(this) }.max() ?: 0)
    }

    override fun visitAggregationFunction(expr: AggregationFunctionExpression): Int {
        return 1 + expr.operands[0].accept(this)
    }

}


private class Compiler(private val mv: MethodVisitor) : ExpressionVisitor<Unit> {

    private val methodStart = Label()
    private val methodEnd = Label()

    override fun visitIdentifier(expr: IdentifierExpression) {
    }

    override fun visitNumericLiteral(expr: NumericLiteralExpression) {
        mv.visitLdcInsn(expr.value)
    }

    override fun visitBooleanLiteral(expr: BooleanLiteralExpression) {
        mv.visitLdcInsn(expr.value)
    }

    override fun visitStringLiteral(expr: StringLiteralExpression) {
        mv.visitLdcInsn(expr.value)
    }

    override fun visitFunction(expr: FunctionExpression) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visitAggregationFunction(expr: AggregationFunctionExpression) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visitColumn(expr: ColumnExpression) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}