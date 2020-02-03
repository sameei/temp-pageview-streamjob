package com.reply.data.interviewtask

import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, KeyedBroadcastProcessFunction}
import org.apache.flink.util.{Collector, OutputTag}
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

class EnrichPageViewWithUserThroughBroadcast(
    usersStateDescriptor: MapStateDescriptor[String, User],
    unhandledViews : OutputTag[PageView]
) extends KeyedBroadcastProcessFunction[String, PageView, User, PageViewEnriched] {

    val logger = LoggerFactory.getLogger(classOf[EnrichPageViewWithUserThroughBroadcast])

    override def processElement(
        value: PageView,
        ctx: KeyedBroadcastProcessFunction[String, PageView, User, PageViewEnriched]#ReadOnlyContext,
        out: Collector[PageViewEnriched]
    ): Unit = {
        val users = ctx.getBroadcastState(usersStateDescriptor)
        users.contains(value.user) match {
            case true =>
                val u = users.get(value.user)
                out.collect(PageViewEnriched(u.user, u.gender, u.region, value.page, value.time))
            case false =>
                logger.warn("NOT DEFINED USER : IGNORING THE PAGE VIEW : {}", value)
                ctx.output(unhandledViews, value)
        }
    }

    override def processBroadcastElement(
        value: User,
        ctx: KeyedBroadcastProcessFunction[String, PageView, User, PageViewEnriched]#Context,
        out: Collector[PageViewEnriched]
    ): Unit = ctx.getBroadcastState(usersStateDescriptor).put(value.user, value)
}
