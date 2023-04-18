package com.verve.assignment.configs

object ApplicationConfig extends BaseConfig {
  lazy val filePath: String = config getString "file.path"
  lazy val clickFilePath: String = filePath + "/" + config.getString("file.clicks.name")
  lazy val impressionFilePath: String = filePath + "/" + config.getString("file.impressions.name")
  lazy val part1FilePath: String = filePath + "/" + config.getString("file.output.part1")
  lazy val part2FilePath: String = filePath + "/" + config.getString("file.output.part2")
}
