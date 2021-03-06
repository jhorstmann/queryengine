package net.jhorstmann.queryengine

import org.junit.jupiter.api.Test
import org.objectweb.asm.ClassVisitor
import org.objectweb.asm.ClassWriter
import org.objectweb.asm.Opcodes
import org.objectweb.asm.util.CheckClassAdapter
import java.io.FileOutputStream
import java.util.function.IntSupplier
import java.util.function.Supplier
import kotlin.test.assertEquals

class AsmExamplesTest {
    object OpenClassLoader : ClassLoader(OpenClassLoader::class.java.classLoader) {

        fun defineClass(name: String, bytes: ByteArray): Class<*> {
            // delegate to protected method
            return OpenClassLoader.defineClass(name, bytes, 0, bytes.size)
        }
    }

    private fun dumpToTargetClasses(name: String, bytes: ByteArray) {
        FileOutputStream("target/classes/$name.class").use {
            it.write(bytes)
        }
    }

    @Test
    fun `should generate empty class`() {
        val cw = ClassWriter(ClassWriter.COMPUTE_FRAMES or ClassWriter.COMPUTE_MAXS)

        val name = "EmptyClass"
        cw.visit(Opcodes.V1_8, Opcodes.ACC_PUBLIC, name, null, "java/lang/Object", emptyArray())

        generateEmptyConstructor(cw)

        val bytes = cw.toByteArray()

        dumpToTargetClasses(name, bytes)

        val emptyClass = OpenClassLoader.defineClass(name, bytes)

        @Suppress("UNCHECKED_CAST")
        val instance = emptyClass.getDeclaredConstructor().newInstance()

        assertEquals(name, instance.javaClass.simpleName)
    }

    @Test
    fun `should generate method adding two integers`() {
        val cw = ClassWriter(ClassWriter.COMPUTE_FRAMES or ClassWriter.COMPUTE_MAXS)

        val name = "IntAdder"
        cw.visit(Opcodes.V1_8, Opcodes.ACC_PUBLIC, name, null, "java/lang/Object", arrayOf("java/util/function/IntSupplier"))

        generateEmptyConstructor(cw)

        val mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "getAsInt", "()I", null, null)

        mv.visitCode()
        mv.visitInsn(Opcodes.ICONST_1)
        mv.visitInsn(Opcodes.ICONST_2)
        mv.visitInsn(Opcodes.IADD)
        mv.visitInsn(Opcodes.IRETURN)

        mv.visitMaxs(2, 0)
        mv.visitEnd()

        val bytes = cw.toByteArray()

        dumpToTargetClasses(name, bytes)

        val intAdder = OpenClassLoader.defineClass(name, bytes)

        @Suppress("UNCHECKED_CAST")
        val instance = intAdder.getDeclaredConstructor().newInstance() as IntSupplier

        assertEquals(3, instance.asInt)
    }


    @Test
    fun `should generate method invoking a constructor`() {
        val cw = ClassWriter(ClassWriter.COMPUTE_FRAMES or ClassWriter.COMPUTE_MAXS)
        val cv = CheckClassAdapter(cw, true)

        val name = "IntegerSupplier"
        cv.visit(Opcodes.V1_8, Opcodes.ACC_PUBLIC, name, null, "java/lang/Object", arrayOf("java/util/function/Supplier"))

        generateEmptyConstructor(cv)

        val mv = cv.visitMethod(Opcodes.ACC_PUBLIC, "get", "()Ljava/lang/Object;", null, null)

        mv.visitCode()
        mv.visitInsn(Opcodes.ICONST_1)
        mv.visitTypeInsn(Opcodes.NEW, "java/lang/Integer")
        mv.visitInsn(Opcodes.DUP_X1)
        mv.visitInsn(Opcodes.SWAP)
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Integer", "<init>", "(I)V", false)
        mv.visitInsn(Opcodes.ARETURN)

        mv.visitMaxs(3, 1)
        mv.visitEnd()

        val bytes = cw.toByteArray()

        dumpToTargetClasses(name, bytes)

        val integerSupplier = OpenClassLoader.defineClass(name, bytes)

        @Suppress("UNCHECKED_CAST")
        val instance = integerSupplier.getDeclaredConstructor().newInstance() as Supplier<Any>

        assertEquals(1, instance.get())
    }

    private fun generateEmptyConstructor(cw: ClassVisitor) {
        val init = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null)

        init.visitCode()
        init.visitVarInsn(Opcodes.ALOAD, 0)
        init.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false)

        init.visitInsn(Opcodes.RETURN)
        init.visitMaxs(1, 1)
        init.visitEnd()
    }

}