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

    private fun deserialize(): DataFileReader<GenericRecord> {
        val datumReader = GenericDatumReader<GenericRecord>(this.schema)
        return DataFileReader<GenericRecord>(this.file, datumReader)
    }

    fun performOperation(key: String, operation: String) {
        when (operation) {
            "sum" -> this.sum(key)
            "avg" -> this.avg(key)
            "min" -> this.max(key)
            "max" -> this.min(key)
        }
    }

    private fun sum(key: String): Float {
        val dataFileReader = this.deserialize()
        return dataFileReader.reduce {r, s -> r + s.get(key) as Float}
    }

    private fun avg(key: String) {
        val dataFileReader = this.deserialize()
        return dataFileReader.reduce {r, s -> r + s.get(key) as Float}
    }

    private fun min(key: String): Float {
        val dataFileReader = this.deserialize()
        val minRecord = dataFileReader.minBy { record -> record.get(key) as Float }
        return minRecord.get(key) as Float
    }

    private fun max(key: String): Float {
        val dataFileReader = this.deserialize()
        val maxRecord = dataFileReader.maxBy { doc -> doc.get(key) as Float }
        return maxRecord.get(key) as Float
    }

    private fun count(key: String) {
        val dataFileReader = this.deserialize()
        return dataFileReader.reduce {r, s -> r + s.get(key) as Float}
    }
}