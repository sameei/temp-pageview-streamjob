package com.reply.data.interviewtask

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration

class PageViewSumToAvroGenericDataRecord extends RichMapFunction[PageViewSum, GenericData.Record]{

    @transient var schema : Schema = null

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)

        schema = Schema.parse(
            """
              |{
              |  "connect.name": "ksql.pageviews",
              |  "fields": [
              |    {
              |      "name": "viewtime",
              |      "type": "long"
              |    },
              |    {
              |      "name": "gender",
              |      "type": "string"
              |    },
              |    {
              |      "name": "pageid",
              |      "type": "string"
              |    },
              |    {
              |      "name": "userscount",
              |      "type": "long"
              |    }
              |  ],
              |  "name": "top_pages",
              |  "namespace": "ksql",
              |  "type": "record"
              |}
              |""".stripMargin)

    }

    override def map(value: PageViewSum): GenericData.Record = {
        val record = new GenericData.Record(schema)
        record.put("gender", value.gender)
        record.put("pageid", value.page)
        record.put("viewtime", value.sumTimes)
        record.put("userscount", value.usersCount)
        println(record)
        record
    }
}
