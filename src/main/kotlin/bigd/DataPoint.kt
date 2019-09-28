package bigd

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.reflect.ReflectData

class DataPoint() : GenericRecord {
    override fun put(i: Int, v: Any?): Unit {}
    override fun put(key: String?, v: Any?): Unit {}
    override fun get(key: String?): Any {
        return 10.0F
    }
    override fun get(i: Int): Any {
        return 10.0F
    }
    override fun getSchema(): Schema {
        return ReflectData.get().getSchema(DataPoint::class.javaObjectType)
    }
}