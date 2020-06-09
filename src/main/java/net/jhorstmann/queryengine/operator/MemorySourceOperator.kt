package net.jhorstmann.queryengine.operator

class MemorySourceOperator(val indices: IntArray, private val values: List<List<Any?>>) : Operator() {
    //private var iterator: Iterator<List<Any?>>? = null
    private val len = values.size
    private var idx = 0
    private val row = arrayOfNulls<Any?>(indices.size)

    override fun open() {
        //this.iterator = values.iterator()
        this.idx = 0
    }

    override fun close() {
        for (j in 0 until indices.size) {
            row[j] = null
        }

        //this.iterator = null
    }

    override fun next(): Array<Any?>? {
        //val iterator = this.iterator ?: throw IllegalStateException("Operator not initialized")

        val i = this.idx
        val indices = this.indices

        if (i < len) {
            this.idx = i+1

            val row = values[i]
            val res = this.row

            for (j in 0 until row.size) {
                res[j] = row[indices[j]]
            }

            return res


            //return Array(indices.size) { row[it] }
        } else {
            return null
        }
    }
}