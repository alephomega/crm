package com.kakaopage.global.crm

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util.{Calendar, Date, TimeZone}

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}


case class SummarizationBuffer(last: Option[Timestamp], frequency: Int, distribution: Distribution)
case class SummarizationResult(last: String, frequency: Int, distribution: Distribution)

object SummarizationBuffer {

  def add(b: SummarizationBuffer, at: Timestamp, timeZone: TimeZone): SummarizationBuffer = {
    val last = b.last match {
      case Some(t: Timestamp) => if (t.before(at)) Some(at) else b.last
      case _ => Some(at)
    }

    val frequency = b.frequency + 1
    val distribution = Distribution.increment(b.distribution, h(at, timeZone), d(at, timeZone))

    SummarizationBuffer(last, frequency, distribution)
  }

  def merge(b1: SummarizationBuffer, b2: SummarizationBuffer): SummarizationBuffer = {
    val last = b1.last match {
      case Some(t1: Timestamp) =>
        b2.last match {
          case Some(t2: Timestamp) => if (t1.before(t2)) Some(t2) else Some(t1)
          case _ => Some(t1)
        }

      case _ =>
        b2.last match {
          case Some(d2: Timestamp) => Some(d2)
          case _ => None
        }
    }

    val frequency = b1.frequency + b2.frequency
    val distribution = Distribution.merge(b1.distribution, b2.distribution)

    SummarizationBuffer(last, frequency, distribution)
  }

  private def h(t: Timestamp, timeZone: TimeZone) = {
    val cal = Calendar.getInstance(timeZone)
    cal.setTime(t)
    cal.get(Calendar.HOUR_OF_DAY)
  }

  private def d(t: Timestamp, timeZone: TimeZone) = {
    val cal = Calendar.getInstance(timeZone)
    cal.setTime(t)
    (cal.get(Calendar.DAY_OF_WEEK) + 5) % 7
  }
}



class Summarizer(tz: String) extends Aggregator[Row, SummarizationBuffer, SummarizationResult] with Serializable {
  val timeZone = TimeZone.getTimeZone(tz)

  override def zero: SummarizationBuffer = new SummarizationBuffer(None, 0, Distribution(hour = Array.fill[Int](24)(0), day = Array.fill[Int](7)(0)))

  override def reduce(b: SummarizationBuffer, row: Row): SummarizationBuffer = {
    SummarizationBuffer.add(b, parse(row.getAs[String]("at")), timeZone)
  }

  override def merge(b1: SummarizationBuffer, b2: SummarizationBuffer): SummarizationBuffer = {
    SummarizationBuffer.merge(b1, b2)
  }

  override def finish(reduction: SummarizationBuffer): SummarizationResult = {
    SummarizationResult(format(reduction.last), reduction.frequency, reduction.distribution)
  }

  override def bufferEncoder: Encoder[SummarizationBuffer] = {
    Encoders.product[SummarizationBuffer]
  }

  override def outputEncoder: Encoder[SummarizationResult] = {
    Encoders.product[SummarizationResult]
  }

  def parse(at: String): Timestamp = new Timestamp(Date.from(Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(at))).getTime)

  def format(opt: Option[Timestamp]): String = opt match {
    case Some(t) =>
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX").withZone(ZoneId.of(timeZone.getID))
      formatter.format(t.toInstant)

    case _ => ""
  }
}

object Summarizer {
  def apply(tz: String): Summarizer = new Summarizer(tz)
}
