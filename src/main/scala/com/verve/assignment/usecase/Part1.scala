package com.verve.assignment.usecase

import com.verve.assignment.domain.{Clicks, EventType, Impressions}
import com.verve.assignment.repository.EventRepository
import org.json4s.DefaultFormats
import org.json4s.native.Json

object Part1 {
  /**
   * 1. Read json files of impressions and clicks
   * 2. Left join impressions with clicks
   * 3. groupBy and reduces to calculate metrics per dimensions
   * 4. Write to json file
  */
  def call(): Unit = {
    implicit val jsonFormat: Json = Json(DefaultFormats)

    val impressionClickLeftJoin = for {
      impression <- EventRepository.get(EventType.Impressions).asInstanceOf[List[Impressions]].filter(_.id.nonEmpty)
    } yield {
      val clicks = EventRepository.get(EventType.Clicks).asInstanceOf[List[Clicks]]
      val click = clicks.find(_.impressionId == impression.id.getOrElse("")).getOrElse(Clicks("", 0)) //left join
      // Data format necessary to calculate next metrics:
      // appId, countryCode, revenue per click, 1 shows impression count, 0|1 means click count
      (impression.appId, impression.countryCode, click.revenue, 1, if (click.impressionId.isEmpty) 0 else 1)
    }
    val metrics = impressionClickLeftJoin
      .groupBy(e => (e._1, e._2))
      .map(e => e._2.reduceLeft((t1, t2) => (t1._1, t1._2, t1._3 + t2._3, t1._4 + t2._4, t1._5 + t2._5))).map { e =>
      Map("app_id" -> e._1, "country_code" -> e._2, "impressions" -> e._4, "clicks" -> e._5, "revenue" -> e._3)
    }.toList

    EventRepository.write(1)(metrics)
  }

}
