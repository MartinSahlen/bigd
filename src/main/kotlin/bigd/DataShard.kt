package bigd

import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import java.io.File


class DataShard(fileName: String) {

    private val file: File = File(fileName)
    private val schema: Schema = Schema.Parser().parse(this.file)

    fun serialize(collection: Collection<GenericRecord>) {
        val datumWriter = GenericDatumWriter<GenericRecord>(this.schema)
        val dataFileWriter = DataFileWriter<GenericRecord>(datumWriter)
        dataFileWriter.create(this.schema, this.file)
        for (doc in collection) {
            dataFileWriter.append(doc)
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

    private fun deserialize(): DataFileReader<GenericRecord> {
        val datumReader = GenericDatumReader<GenericRecord>(this.schema)
        return DataFileReader<GenericRecord>(this.file, datumReader)
    }

    private fun getValues(key: String): List<Float> {
        val dataFileReader = this.deserialize()
        return dataFileReader.map {r -> r.get(key) as Float}
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