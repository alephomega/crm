package com.kakaopage.global.crm

import org.apache.spark.sql.Row


case class Distribution(hour: Array[Int], day: Array[Int]) {

  def toRow(): Row = {
    Row(hour, day)
  }

  def increment(h: Int, d: Int) = {
    hour.update(h, hour(h) + 1)
    day.update(d, day(d) + 1)
  }

  def merge(distribution: Distribution) = {
    Array.tabulate(hour.length) {
      i => hour.update(i, hour(i) + distribution.hour(i))
    }

    Array.tabulate(day.length) {
      i => day.update(i, hour(i) + distribution.day(i))
    }
  }
}