package com.verve.assignment

import com.typesafe.scalalogging.Logger
import com.verve.assignment.configs.ApplicationConfig.filePath
import com.verve.assignment.usecase.Part1
import com.verve.assignment.usecase.Part2
import org.json4s.DefaultFormats
import org.json4s.native.Json

object Main {
  implicit val jsonFormat: Json = Json(DefaultFormats)

  def main(args: Array[String]): Unit = {
    val logger: Logger = Logger("Application")
    Part1.call()
    Part2.call()
  }
}
