package com.ignition.samples

import org.apache.spark.streaming.Seconds
import com.ignition.{ ExecutionException, frame, stream }
import com.ignition.frame._
import com.ignition.types._
import com.ignition.script._
import com.ignition.SparkHelper
import com.ignition.frame.mllib.ColumnStats
import com.ignition.frame.BasicAggregator._
import com.ignition.frame.JoinType._
import com.ignition.frame.mllib.Correlation
import com.ignition.frame.mllib.CorrelationMethod._
import com.ignition.frame.mllib.Regression
import com.ignition.frame.mllib.RegressionMethod._
import org.apache.spark.sql.hive._
import org.apache.spark.sql._

object DataCleanupFlow extends App {

  implicit val rt = new DefaultSparkRuntime(SparkHelper.sqlContext)

  val TIME_STEP = 900000
  val OUTLIER_THRESHOLD = 25

  // reads from CSV file and converts the time to milliseconds
  val readFile = FrameSubProducer {
    val schema = string("timestamp") ~ string("flags") ~ string("status") ~ int("value")
    val csv = CsvFileInput("/Users/vladorz/Downloads/lights.csv") separator "," schema schema
    val parse = Formula("time" -> "PARSE(timestamp, \"dd-MMM-yy h:mm:ss a z\")".mvel)
    val normalize = SQLQuery(s"""
      | SELECT
      |   floor(CAST(time AS BIGINT) / ($TIME_STEP / 1000)) * $TIME_STEP AS millis,
      |   HOUR(time) * 60 + MINUTE(time) AS minute,
      |   QUARTER(time) AS quarter,
      |   MONTH(time) AS month,
      |   value AS value
      | FROM input0
      | WHERE value != 0""".stripMargin)
    val coalesce = Repartition(12)

    csv --> parse --> normalize --> coalesce
  }

  // compute inter-row deltas and filter outliers
  val computeDeltas = FrameSubTransformer {
    val diff = SQLQuery(s"""
      | SELECT
      |   millis, value, next_millis, next_value,
      |   next_millis - millis AS delta_millis,
      |   next_value - value AS delta_value 
      | FROM (
      |   SELECT
      |     millis,
      |     value,
      |     LEAD(value) OVER (ORDER BY millis) AS next_value,
      |     LEAD(millis) OVER (ORDER BY millis) AS next_millis
      |   FROM input0) tmp
      |   WHERE ABS(next_value - value) <= $OUTLIER_THRESHOLD""".stripMargin)
    val cache = Cache()

    diff --> cache

    (diff.in(0), cache)
  }

  readFile --> computeDeltas

  // find time range 
  val timeRange = ColumnStats("millis")

  computeDeltas --> timeRange

  val minMillis = timeRange.output(0).head.getAs[Double]("millis_min").toLong
  val maxMillis = timeRange.output(0).head.getAs[Double]("millis_max").toLong

  // join with reference time set and fill the gaps
  val joinAndFill = FrameSubTransformer {
    val timeRef = RangeInput(minMillis, maxMillis, TIME_STEP)

    val join = SQLQuery("""
      | SELECT
      |   CAST(input0.id AS timestamp) AS time,
      |   id != millis AS gap,
      |   id, delta_value, delta_millis,
      |   IF(id = millis, value, (id - millis) * delta_value / delta_millis + value) AS value
      | FROM input0, input1
      | WHERE 
      |   input0.id >= input1.millis AND input0.id < input1.next_millis
      | ORDER BY input0.id""".stripMargin)

    timeRef --> join.in(0)

    (join.in(1), join)
  }

  val writeToFile = TextFileOutput("/tmp/lights_corr_millis.csv") % ("time") % ("value" -> "%f")

  computeDeltas --> joinAndFill --> writeToFile
  
  writeToFile.evaluate
}