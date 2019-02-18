package com.kakaopage.global.crm

import java.util

import com.typesafe.config.{Config, ConfigFactory}

class Filter(val config: Config) {
  val ignore: util.List[String] = config.getStringList("filter.events")
  val predicate: (String) => Boolean = (event: String) => !ignore.contains(event)
}