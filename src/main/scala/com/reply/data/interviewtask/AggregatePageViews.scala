package com.reply.data.interviewtask

import com.reply.data.interviewtask
import org.apache.flink.api.common.functions.AggregateFunction

class AggregatePageViews
    extends AggregateFunction[PageViewEnriched, PageViewAggr, PageViewAggr] {

    override def createAccumulator(): PageViewAggr =
        PageViewAggr("", "", -1, -1, -1, -1)

    override def add(value: PageViewEnriched, accumulator: PageViewAggr): PageViewAggr = {

        // println(value, accumulator)

        if (accumulator.page.isEmpty)
            PageViewAggr(
                value.page, value.gender,
                value.time, value.time, 1, -1
            )
        else accumulator.copy(
            from = Math.min(value.time, accumulator.from),
            to = Math.max(value.time, accumulator.to),
            count = accumulator.count + 1
        )
    }

    override def getResult(accumulator: PageViewAggr): PageViewAggr = {
        accumulator
    }

    override def merge(a: PageViewAggr, b: PageViewAggr): PageViewAggr = {
        PageViewAggr(
            a.page, a.gender,
            Math.min(a.from, b.from),
            Math.max(a.to, b.to),
            a.count + b.count,
            -1
        )
    }
}
