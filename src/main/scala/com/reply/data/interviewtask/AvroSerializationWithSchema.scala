package com.reply.data.interviewtask

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.flink.api.common.serialization.SerializationSchema

class AvroSerializationWithSchema extends SerializationSchema[GenericData.Record] {
    override def serialize(record: GenericData.Record): Array[Byte] = {
        val schema = record.getSchema
        val outputStream = new ByteArrayOutputStream()
        val datumWriter = new GenericDatumWriter[GenericRecord](schema)
        val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
        dataFileWriter.create(schema, outputStream)
        dataFileWriter.append(record)
        dataFileWriter.flush()
        dataFileWriter.close()
        outputStream.toByteArray
    }
}
