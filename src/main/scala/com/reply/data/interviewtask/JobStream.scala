package com.reply.data.interviewtask


import java.util.Properties

import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.Collector


import org.apache.avro.Schema

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object JobStream {

    def main(args: Array[String]): Unit = {

        val env =
            StreamExecutionEnvironment.getExecutionEnvironment

        env.setStateBackend(new MemoryStateBackend(true))
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

        val usersKC = {
            val deserializer =
                ConfluentRegistryAvroDeserializationSchema.forGeneric(userSchema, CONFLUENT_SCHEMA_REGISTERY)

            new FlinkKafkaConsumer[GenericRecord]("users", deserializer, consumerConfig)
        }

        val pageviewsKC = {
            val deserializer =
                ConfluentRegistryAvroDeserializationSchema.forGeneric(pageviewSchema, CONFLUENT_SCHEMA_REGISTERY)

            new FlinkKafkaConsumer[GenericRecord]("pageviews", deserializer, consumerConfig)

        }

        val topsKP =
            new FlinkKafkaProducer[GenericData.Record](
                KAFKA_BROKER_LIST, "top_pages",
                new AvroSerializationWithSchema()
            )

        val users =
            env.addSource(usersKC).map { r =>
                User(
                    r.get("userid").asInstanceOf[org.apache.avro.util.Utf8].toString,
                    r.get("gender").asInstanceOf[org.apache.avro.util.Utf8].toString,
                    r.get("regionid").asInstanceOf[org.apache.avro.util.Utf8].toString,
                    r.get("registertime").asInstanceOf[Long]
                )
            }

        val pageviews =
            env.addSource(pageviewsKC).map { r =>
                PageView(
                    r.get("pageid").asInstanceOf[org.apache.avro.util.Utf8].toString,
                    r.get("userid").asInstanceOf[org.apache.avro.util.Utf8].toString,
                    r.get("viewtime").asInstanceOf[Long]
                )
            }

        val usersSt = new MapStateDescriptor(
            "users",
            createTypeInformation[String],
            createTypeInformation[User]
        );

        val usersBroadcastStream = users.broadcast(usersSt)

        val unhandledViews = OutputTag[PageView]("unhandledViews")

        pageviews.keyBy {_.user}
            .connect(usersBroadcastStream) // Could be replaced by JOIN based on the contents of the users stream
            .process(new EnrichPageViewWithUserThroughBroadcast(usersSt, unhandledViews))
            .keyBy { i => i.page -> i.gender }
            .timeWindow(Time.minutes(1), Time.seconds(10))
            .apply { (key: (String, String), tw: TimeWindow, list: Iterable[PageViewEnriched], out: Collector[PageViewSum]) =>
                val users = list.groupBy(_.user).keys
                val userCount =  users.size // replace with bloomfilter
                val sum = list.map(_.time).sum // could be done with one loop alongside deduplicating users
                out collect PageViewSum(key._1, key._2, sum, userCount, tw.getStart, tw.getEnd)
                println(users, userCount)
            }
            .keyBy(_.gender)
            .timeWindow(Time.minutes(1))
            .apply { (key: String, tw: TimeWindow, list: Iterable[PageViewSum], out: Collector[PageViewSum]) =>
                // could be replaced with an window function implementation to store less state
                val top10 = list.toSeq.sortBy(_.sumTimes).takeRight(10)
                top10.foreach(out.collect)
            }
            .map(new PageViewSumToAvroGenericDataRecord)
            .addSink(topsKP)

        env.execute(":)")
    }

    val userSchema = Schema.parse(
        """
          |{
          |  "connect.name": "ksql.users",
          |  "fields": [
          |    {
          |      "name": "registertime",
          |      "type": "long"
          |    },
          |    {
          |      "name": "userid",
          |      "type": "string"
          |    },
          |    {
          |      "name": "regionid",
          |      "type": "string"
          |    },
          |    {
          |      "name": "gender",
          |      "type": "string"
          |    }
          |  ],
          |  "name": "users",
          |  "namespace": "ksql",
          |  "type": "record"
          |}
          |""".stripMargin)

    val pageviewSchema = Schema.parse(
        """
          |{
          |  "connect.name": "ksql.pageviews",
          |  "fields": [
          |    {
          |      "name": "viewtime",
          |      "type": "long"
          |    },
          |    {
          |      "name": "userid",
          |      "type": "string"
          |    },
          |    {
          |      "name": "pageid",
          |      "type": "string"
          |    }
          |  ],
          |  "name": "pageviews",
          |  "namespace": "ksql",
          |  "type": "record"
          |}
          |""".stripMargin)

    val CONFLUENT_SCHEMA_REGISTERY = "http://localhost:8081"

    val KAFKA_BROKER_LIST = "localhost:9092"

    def consumerConfig = {
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "localhost:2181")
        properties.setProperty("group.id", "test")
        properties
    }

}
