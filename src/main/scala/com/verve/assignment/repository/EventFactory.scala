package com.verve.assignment.repository

import com.verve.assignment.domain.EventType.EventType
import com.verve.assignment.domain.{Clicks, Event, EventType, Impressions}
import org.json4s.JsonAST.JValue
import org.json4s.DefaultFormats
import org.json4s.Formats
import org.json4s.native.Json

object EventFactory {
  implicit val formats: Formats = DefaultFormats
  implicit val jsonFormat: Json = Json(DefaultFormats)

  def event(name: EventType)(dto: JValue): List[Event] =
    if (name.equals(EventType.Clicks)) dto.camelizeKeys.extract[List[Clicks]] else dto.camelizeKeys.extract[List[Impressions]]

  def json(dto: List[Map[String, Any]]): String =
    jsonFormat.writePretty(dto)
}
