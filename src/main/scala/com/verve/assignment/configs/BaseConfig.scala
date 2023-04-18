package com.verve.assignment.configs

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

trait BaseConfig {
  val config: Config = ConfigFactory.load().withFallback(ConfigFactory.defaultApplication()).resolve()
}
