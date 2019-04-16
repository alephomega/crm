package com.kakaopage.global.crm

import org.apache.spark.sql.Row


case class Distribution(hour: Array[Int] = Array.fill[Int](24)(0), day: Array[Int] = Array.fill[Int](7)(0)) {

  def toRow: Row = {
    Row(hour, day)
  }
}

object Distribution {

  def merge(d1: Distribution, d2: Distribution): Distribution = {
    val d = Distribution()
    Array.tabulate(d.hour.length) {
      i => d.hour.update(i, d1.hour(i) + d2.hour(i))
    }

    Array.tabulate(d.day.length) {
      i => d.day.update(i, d1.day(i) + d2.day(i))
    }

    d
  }

  def increment(distribution: Distribution, h: Int, d: Int): Distribution = {
    distribution.hour.update(h, distribution.hour(h) + 1)
    distribution.day.update(d, distribution.day(d) + 1)
    distribution
  }
}