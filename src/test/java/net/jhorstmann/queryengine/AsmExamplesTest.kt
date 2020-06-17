package net.jhorstmann.queryengine

import org.junit.jupiter.api.Test
import org.objectweb.asm.ClassWriter
import org.objectweb.asm.Opcodes
import java.io.FileOutputStream
import kotlin.test.assertEquals

class AsmExamplesTest {
    object OpenClassLoader : ClassLoader(OpenClassLoader::class.java.classLoader) {

        fun defineClass(name: String, bytes: ByteArray): Class<*> {
            // delegate to protected method
            return OpenClassLoader.defineClass(name, bytes, 0, bytes.size)
        }
    }



    @Test
    fun `should generate empty class`() {
        val cw = ClassWriter(ClassWriter.COMPUTE_FRAMES or ClassWriter.COMPUTE_MAXS)

        val name = "EmptyClass"
        cw.visit(Opcodes.V1_8, Opcodes.ACC_PUBLIC, name, null, "java/lang/Object", emptyArray())

        val init = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null)

        init.visitCode()
        init.visitVarInsn(Opcodes.ALOAD, 0)
        init.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false)

        init.visitInsn(Opcodes.RETURN)
        init.visitMaxs(1, 1)
        init.visitEnd()

        val bytes = cw.toByteArray()

        dumpToTargetClasses(name, bytes)

        val emptyClass = OpenClassLoader.defineClass(name, bytes)

        @Suppress("UNCHECKED_CAST")
        val instance = emptyClass.getDeclaredConstructor().newInstance()

        assertEquals(name, instance.javaClass.simpleName)
    }

    private fun dumpToTargetClasses(name: String, bytes: ByteArray?) {
        FileOutputStream("target/classes/$name.class").use {
            it.write(bytes)
        }
    }
}