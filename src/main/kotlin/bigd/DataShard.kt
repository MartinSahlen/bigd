package bigd

import java.io.File

class DataShard<E>(private val fileName: String) {

    val file = File(fileName)

    fun performOperation(key: String, operation: String) {
        when (operation) {
            "sum" -> this.sum(key)
            "avg" -> this.avg(key)
            "min" -> this.max(key)
            "max" -> this.min(key)
            "count" -> this.count(key)
        }
    }

    private fun getValues(key: String): List<Float> {
        return listOf(0F)
       // return reader.map {r -> r.get(key) as Float}
    }

    private fun sum(key: String): Float {
        return this.getValues(key).sum()
    }

    private fun avg(key: String): AvgStruct {
        return AvgStruct(0F,0)
    }

    private fun min(key: String): Float {
        //val minRecord = dataFileReader.minBy { record -> record.get(key) as Float }
        return 0F
    }

    private fun max(key: String): Float {
       // val maxRecord = dataFileReader.maxBy { doc -> doc.get(key) as Float }
        return 0F
        //return maxRecord?.get(key) as Float
    }

    private fun count(key: String): Int {
        return this.getValues(key).size
    }

    internal class AvgStruct(val sum: Float, val count: Int) {
        fun getValue(): Float {
            return this.sum / this.count
        }
    }
}