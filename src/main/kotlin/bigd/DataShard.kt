package bigd

import java.lang.Double.parseDouble
import java.lang.Float.parseFloat


class DataShard(private val fileName: String, private val offset: Long, private val limit: Long) {

    val fileUtil = FileUtil()

    fun performOperation(key: String, operation: String): Double {
        when (operation) {
            "sum" -> return this.sum(key)
//            "avg" -> return this.avg(key)
            "min" -> return this.min(key)
            "max" -> return this.max(key)
//            "count" -> return this.count(key)
        }
        return 0.0
    }


    private fun sum(key: String): Double {
        return fileUtil
                .readFile(this.fileName, this.offset, this.limit)
                .map {  parseDouble(it.get(key).asText()) }
                .reduce { sum, element -> sum + element }
                .get()
    }

    private fun avg(key: String): AvgStruct {
        return AvgStruct(0F,0)
    }

    private fun min(key: String): Double {
        return fileUtil
                .readFile(this.fileName, this.offset, this.limit)
                .map {  parseDouble(it.get(key).asText()) }
                .min(Double::compareTo)
                .get()
    }

    private fun max(key: String): Double {
        return fileUtil
                .readFile(this.fileName, this.offset, this.limit)
                .map {  parseDouble(it.get(key).asText()) }
                .max(Double::compareTo)
                .get()
    }

    private fun count(key: String): Int {
        return 0 //this.getValues(key).size
    }

    internal class AvgStruct(val sum: Float, val count: Int) {
        fun getValue(): Float {
            return this.sum / this.count
        }
    }
}