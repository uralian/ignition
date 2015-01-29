package com.ignition.data

import scala.Array.canBuildFrom
import org.joda.time.{ DateTime, DateTimeZone }
import org.junit.runner.RunWith
import org.scalacheck.{ Arbitrary, Gen }
import org.slf4j.LoggerFactory
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import com.eaio.uuid.UUID
import com.ignition.data.DataType.BooleanDataType
import java.nio.ByteBuffer

@RunWith(classOf[JUnitRunner])
class DataRowSpec extends Specification {

  val log = LoggerFactory.getLogger(getClass)

  val uuid = new UUID
  val date = DateTime.now
  val binary = Array[Byte](10, 15, 22)

  val columns = Vector("a", "b", "c", "d", "e", "f", "g", "h")
  val data = Vector(true, "123", 5, 22.33, BigDecimal("100"), date, uuid, binary)

  val row = DataRow(columns, data)

  "DataRow.getBoolean" should {
    "take index" in row.getBoolean(0) === true
    "take name" in row.getBoolean("a") === true
    "fail for bad index" in { row.getBoolean(10) must throwA[RuntimeException] }
    "fail for bad name" in { row.getBoolean("z") must throwA[RuntimeException] }
  }

  "DataRow.getString" should {
    "take index" in row.getString(1) === "123"
    "take name" in row.getString("b") === "123"
    "fail for bad index" in { row.getString(10) must throwA[RuntimeException] }
    "fail for bad name" in { row.getString("z") must throwA[RuntimeException] }
  }

  "DataRow.getInt" should {
    "take index" in row.getInt(2) === 5
    "take name" in row.getInt("c") === 5
    "fail for bad index" in { row.getInt(10) must throwA[RuntimeException] }
    "fail for bad name" in { row.getInt("z") must throwA[RuntimeException] }
  }

  "DataRow.getDouble" should {
    "take index" in row.getDouble(3) === 22.33
    "take name" in row.getDouble("d") === 22.33
    "fail for bad index" in { row.getDouble(10) must throwA[RuntimeException] }
    "fail for bad name" in { row.getDouble("z") must throwA[RuntimeException] }
  }

  "DataRow.getDecimal" should {
    "take index" in row.getDecimal(4) === BigDecimal("100")
    "take name" in row.getDecimal("e") === BigDecimal("100")
    "fail for bad index" in { row.getDecimal(10) must throwA[RuntimeException] }
    "fail for bad name" in { row.getDecimal("z") must throwA[RuntimeException] }
  }

  "DataRow.getDateTime" should {
    "take index" in row.getDateTime(5) === date
    "take name" in row.getDateTime("f") === date
    "fail for bad index" in { row.getDateTime(10) must throwA[RuntimeException] }
    "fail for bad name" in { row.getDateTime("z") must throwA[RuntimeException] }
  }

  "DataRow.getUUID" should {
    "take index" in row.getUUID(6) === uuid
    "take name" in row.getUUID("g") === uuid
    "fail for bad index" in { row.getUUID(10) must throwA[RuntimeException] }
    "fail for bad name" in { row.getUUID("z") must throwA[RuntimeException] }
  }

  "DataRow.getBinary" should {
    "take index" in row.getBinary(7) === binary
    "take name" in row.getBinary("h") === binary
    "fail for bad index" in { row.getBinary(10) must throwA[RuntimeException] }
    "fail for bad name" in { row.getBinary("z") must throwA[RuntimeException] }
  }
}