package com.kakaopage.global.crm

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.Row

class Aggregation(val timezone: String) extends Serializable {

  var last: Option[Date] = None
  var frequency: Int = 0
  var distribution = Distribution(hour = Array.fill[Int](24)(0), day = Array.fill[Int](7)(0))

  def aggregate(at: String) = {
    val t = Aggregation.parse(at)

    last = last match {
      case Some(v: Date) => if (v.before(t)) Some(t) else last
      case _ => Some(t)
    }

    frequency += 1
    distribution.increment(h(t), d(t))
  }

  def aggregate(summary: Summary) = {
    val t = Aggregation.parse(summary.last)

    last = last match {
      case Some(v: Date) => if (v.before(t)) Some(t) else last
      case _ => Some(t)
    }

    frequency += summary.frequency
    distribution.merge(summary.distribution)
  }

  def toRow: Row = Row(Aggregation.format(last.orNull), frequency, distribution.toRow())

  private def h(t: Date) = {
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timezone))
    cal.setTime(t)
    cal.get(Calendar.HOUR_OF_DAY)
  }

  private def d(t: Date) = {
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timezone))
    cal.setTime(t)
    (cal.get(Calendar.DAY_OF_WEEK) + 5) % 7
  }
}

object Aggregation {

  val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ssXXX")

  def apply(timezone: String): Aggregation = new Aggregation(timezone)

  def parse(at: String): Date = if (at == null) null else dateFormat.parse(at)

  def format(d: Date): String = if (d == null) null else dateFormat.format(d)

  def aggregate(aggregation: Aggregation, at: String): Aggregation = {
    aggregation.aggregate(at)
    aggregation
  }

  def aggregate(aggregation: Aggregation, summary: Summary): Aggregation = {
    aggregation.aggregate(summary)
    aggregation
  }
}