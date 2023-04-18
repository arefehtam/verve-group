package com.verve.assignment.repository

import com.verve.assignment.configs.ApplicationConfig
import com.verve.assignment.domain.Event
import com.verve.assignment.domain.EventType
import org.json4s.native.JsonMethods._
import EventRepository.{clickFilePath, impressionFilePath, part1FilePath, part2FilePath}
import com.verve.assignment.domain.EventType.EventType

import java.io.File
import java.io.IOException
import java.io.PrintWriter
import scala.io.Source

trait EventRepository {
  def get(name: EventType): List[Event] = {
    val eventStr = (if (name.equals(EventType.Clicks)) Source.fromFile(clickFilePath) else Source.fromFile(impressionFilePath)).getLines().mkString
    val eventObj = parse(eventStr)
    EventFactory.event(name)(eventObj)
  }

  def write(part: Integer)(data: List[Map[String, Any]]): Unit = try {
    val json = EventFactory json data
    val file = if (part == 1) new File(part1FilePath) else new File(part2FilePath)
    val writer = new PrintWriter(file)
    writer.write(json)
    writer.close()
  } catch {
    case io: IOException => throw io.fillInStackTrace
    case e: Exception => throw e.fillInStackTrace
  }
}

object EventRepository extends EventRepository {
  val clickFilePath: String = ApplicationConfig.clickFilePath
  val impressionFilePath: String = ApplicationConfig.impressionFilePath
  val part1FilePath: String = ApplicationConfig.part1FilePath
  val part2FilePath: String = ApplicationConfig.part2FilePath
}
