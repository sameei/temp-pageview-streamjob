package com.reply.data.interviewtask

// [info] Here: {"registertime": 1516315735019, "userid": "User_4", "regionid": "Region_4", "gender": "FEMALE"}
case class User(user: String, gender: String, region: String, registerationTime: Long)

case class PageView(page: String, user: String, time: Long)

case class PageViewEnriched(user: String, gender: String, region: String, page: String, time: Long)

case class PageViewSum(page: String, gender: String, sumTimes: Long, usersCount: Long, from: Long, to: Long)

case class PageViewAggr(page: String, gender: String, from: Long, to: Long, count: Int, rank: Int)
