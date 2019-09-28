package bigd

import java.lang.Double.parseDouble
import java.lang.Float.parseFloat


class DataShard(private val fileName: String, private val offset: Long, private val limit: Long) {

    val fileUtil = FileUtil()

    fun performOperation(key: String, operation: String): Double {
        when (operation) {
            "sum" -> return this.sum(key)
//            "avg" -> return this.avg(key)
//            "min" -> return this.max(key)
//            "max" -> return this.min(key)
//            "count" -> return this.count(key)
        }
        return 0.0
    }


    private fun sum(key: String): Double {
        return fileUtil
                .readFile(this.fileName, this.offset, this.limit)
                .map {  parseDouble(it.get(key).toString().replace("\"", "")) }
                .reduce { sum, element -> sum + element }
                .get()
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
        return 0 //this.getValues(key).size
    }

    internal class AvgStruct(val sum: Float, val count: Int) {
        fun getValue(): Float {
            return this.sum / this.count
        }
    }
}