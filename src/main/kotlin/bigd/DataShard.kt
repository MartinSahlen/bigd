package bigd

import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.reflect.ReflectDatumReader
import org.apache.avro.reflect.ReflectDatumWriter
import java.io.File

class DataShard<E:GenericRecord>(private val clazz: Class<E>, private val fileName: String) {

    val file = File(fileName)

    fun serialize(collection: Collection<E>) {
        val userDatumWriter = ReflectDatumWriter(clazz)
        val dataFileWriter = DataFileWriter(userDatumWriter)
        for (doc in collection) {
            dataFileWriter.create(doc.schema, File(fileName))
        }
        dataFileWriter.close()
    }

    fun performOperation(key: String, operation: String) {
        when (operation) {
            "sum" -> this.sum(key)
            "avg" -> this.avg(key)
            "min" -> this.max(key)
            "max" -> this.min(key)
        }
    }

    private fun deserialize(): DataFileReader<E> {
        val reader = ReflectDatumReader(clazz)
        return DataFileReader<E>(file, reader)
    }

    private fun getValues(key: String): List<Float> {
        val reader = this.deserialize()
        return reader.map {r -> r.get(key) as Float}
    }

    private fun sum(key: String): Float {
        return this.getValues(key).sum()
    }

    private fun avg(key: String): AvgStruct {
        val values = this.getValues(key)
        return AvgStruct(values.sum(), values.size)
    }

    private fun min(key: String): Float {
        val dataFileReader = this.deserialize()
        val minRecord = dataFileReader.minBy { record -> record.get(key) as Float }
        return minRecord?.get(key) as Float
    }

    private fun max(key: String): Float {
        val dataFileReader = this.deserialize()
        val maxRecord = dataFileReader.maxBy { doc -> doc.get(key) as Float }
        return maxRecord?.get(key) as Float
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