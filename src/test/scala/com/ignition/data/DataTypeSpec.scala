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

import DataType.{ BinaryDataType, boolean2bytes, double2bytes, int2bytes, long2bytes }

@RunWith(classOf[JUnitRunner])
class DataTypeSpec extends Specification with ScalaCheck {

  val log = LoggerFactory.getLogger(getClass)

  implicit lazy val arbDateTime: Arbitrary[DateTime] = {
    val lbound = DateTime.parse("2000-01-01")
    val ubound = DateTime.parse("2050-01-01")
    val dateTimeGen: Gen[DateTime] = for {
      msec <- Gen.choose(lbound.getMillis, ubound.getMillis)
      offset <- Gen.choose(-23, 23)
    } yield new DateTime(msec) withZone (DateTimeZone.forOffsetHours(offset))
    Arbitrary(dateTimeGen)
  }

  implicit lazy val arbUUID: Arbitrary[UUID] = {
    val uuidGen = Arbitrary.arbitrary[Int] map (_ => new UUID)
    Arbitrary(uuidGen)
  }

  implicit lazy val arbByte: Arbitrary[Byte] = Arbitrary(Gen.choose[Byte](0, 127))

  implicit lazy val arbBigDecimal: Arbitrary[BigDecimal] =
    Arbitrary(Gen.choose(-1000000.0, 1000000.0) map BigDecimal.apply)

  "String data type" should {
    "convert from Int" in prop { (x: Int) => checkConversion[Int, String](x, x.toString) }
    "convert from Double" in prop { (x: Double) => checkConversion[Double, String](x, x.toString) }
    "convert from BigDecimal" in prop { (x: BigDecimal) => checkConversion[BigDecimal, String](x, x.toString) }
    "convert from Boolean" in prop { (x: Boolean) => checkConversion[Boolean, String](x, x.toString) }
    "convert from DateTime" in prop { (x: DateTime) => checkConversion[DateTime, String](x, x.toString) }
    "convert from UUID" in prop { (x: UUID) => checkConversion[UUID, String](x, x.toString) }
    "convert from String" in prop { (x: String) => checkConversion[String, String](x, x) }
    "convert from null" in checkNull[String]
  }

  "Boolean data type" should {
    "convert from Boolean" in prop { (x: Boolean) => checkConversion[Boolean, Boolean](x, x) }
    "convert from Int" in prop { (x: Int) => checkConversion[Int, Boolean](x, x != 0) }
    "convert from Double" in prop { (x: Double) => checkConversion[Double, Boolean](x, x != 0) }
    "convert from BigDecimal" in prop { (x: BigDecimal) => checkConversion[BigDecimal, Boolean](x, x != 0) }
    "convert from DateTime" in prop { (x: DateTime) => checkConversion[DateTime, Boolean](x, x.getMillis != 0) }
    "convert from Array[Byte]" in prop { (x: Array[Byte]) => checkConversion[Array[Byte], Boolean](x, x.exists(_ != 0)) }
    "convert from UUID" in prop { (x: UUID) => checkConversion[UUID, Boolean](x, x.hashCode != 0) }
    "convert from String" in prop { (x: String) => checkConversion[String, Boolean](x, x.toLowerCase == "true") }
    "convert from null" in checkNull[Boolean]
  }

  "Int data type" should {
    "convert from Int" in prop { (x: Int) => checkConversion[Int, Int](x, x) }
    "convert from Double" in prop { (x: Double) => checkConversion[Double, Int](x, x.intValue) }
    "convert from BigDecimal" in prop { (x: BigDecimal) => checkConversion[BigDecimal, Int](x, x.intValue) }
    "convert from Boolean" in prop { (x: Boolean) => checkConversion[Boolean, Int](x, if (x) 1 else 0) }
    "convert from DateTime" in prop { (x: DateTime) => checkConversion[DateTime, Int](x, x.getMillis.toInt) }
    "convert from UUID" in prop { (x: UUID) => checkConversion[UUID, Int](x, x.hashCode) }
    "convert from Array[Byte]" in prop { (b0: Byte, b1: Byte, b2: Byte, b3: Byte) =>
      checkConversion[Array[Byte], Int](Array(b0), b0.toInt)
      checkConversion[Array[Byte], Int](Array(b0, b1), shift(b0, 8) | b1)
      checkConversion[Array[Byte], Int](Array(b0, b1, b2), shift(b0, 16) | shift(b1, 8) | b2)
      checkConversion[Array[Byte], Int](Array(b0, b1, b2, b3), shift(b0, 24) | shift(b1, 16) | shift(b2, 8) | b3)
    }
    "convert from String" in prop { (x: Int) => checkConversion[String, Int](x.toString, x) }
    "convert from null" in checkNull[Int]
  }

  "Double data type" should {
    "convert from Int" in prop { (x: Int) => checkConversion[Int, Double](x, x.toDouble) }
    "convert from Double" in prop { (x: Double) => checkConversion[Double, Double](x, x) }
    "convert from BigDecimal" in prop { (x: BigDecimal) => checkConversion[BigDecimal, Double](x, x.doubleValue) }
    "convert from Boolean" in prop { (x: Boolean) => checkConversion[Boolean, Double](x, if (x) 1.0 else 0.0) }
    "convert from DateTime" in prop { (x: DateTime) => checkConversion[DateTime, Double](x, x.getMillis.toDouble) }
    "convert from UUID" in prop { (x: UUID) => checkConversion[UUID, Double](x, x.hashCode) }
    "convert from String" in prop { (x: Double) => checkConversion[String, Double](x.toString, x) }
    "convert from null" in checkNull[Double]
  }

  "Decimal data type" should {
    "convert from Int" in prop { (x: Int) => checkConversion[Int, BigDecimal](x, x) }
    "convert from Double" in prop { (x: Double) => checkConversion[Double, BigDecimal](x, x) }
    "convert from BigDecimal" in prop { (x: BigDecimal) => checkConversion[BigDecimal, BigDecimal](x, x) }
    "convert from Boolean" in prop { (x: Boolean) => checkConversion[Boolean, Double](x, if (x) 1 else 0) }
    "convert from DateTime" in prop { (x: DateTime) => checkConversion[DateTime, BigDecimal](x, x.getMillis) }
    "convert from UUID" in prop { (x: UUID) => checkConversion[UUID, BigDecimal](x, x.hashCode) }
    "convert from String" in prop { (x: BigDecimal) => checkConversion[String, BigDecimal](x.toString, x) }
    "convert from null" in checkNull[BigDecimal]
  }

  "DateTime data type" should {
    "convert from Int" in prop { (x: Int) => checkConversion[Int, DateTime](x, new DateTime(x.longValue)) }
    "convert from Double" in prop { (x: Int) => checkConversion[Double, DateTime](x.doubleValue, new DateTime(x.longValue)) }
    "convert from BigDecimal" in prop { (x: BigDecimal) => checkConversion[BigDecimal, DateTime](x, new DateTime(x.longValue)) }
    "convert from DateTime" in prop { (x: DateTime) => checkConversion[DateTime, DateTime](x, x) }
    "convert from UUID" in prop { (x: UUID) => checkConversion[UUID, DateTime](x, new DateTime(x.getTime)) }
    "convert from String" in prop { (x: DateTime) => checkConversion[String, DateTime](x.toString, x) }
    "convert from null" in checkNull[DateTime]
  }

  "UUID data type" should {
    "convert from UUID" in prop { (x: UUID) => checkConversion[UUID, UUID](x, x) }
    "convert from String" in prop { (x: UUID) => checkConversion[String, UUID](x.toString, x) }
    "convert from null" in checkNull[UUID]
  }

  "Binary data type" should {
    import DataType._
    "convert from Int" in prop { (x: Int) => checkConversion[Int, Array[Byte]](x, x: Array[Byte]) }
    "convert from Double" in prop { (x: Double) => checkConversion[Double, Array[Byte]](x, x: Array[Byte]) }
    "convert from BigDecimal" in prop { (x: BigDecimal) =>
      checkConversion[BigDecimal, Array[Byte]](x,
        x.bigDecimal.unscaledValue.toByteArray)
    }
    "convert from Boolean" in prop { (x: Boolean) => checkConversion[Boolean, Array[Byte]](x, x: Array[Byte]) }
    "convert from DateTime" in prop { (x: DateTime) => checkConversion[DateTime, Array[Byte]](x, x.getMillis: Array[Byte]) }
    "convert from UUID" in prop { (x: UUID) =>
      checkConversion[UUID, Array[Byte]](x,
        (x.getClockSeqAndNode: Array[Byte]) ++ (x.getTime: Array[Byte]))
    }
    "convert from Array[Byte]" in prop { (x: Array[Byte]) => checkConversion[Array[Byte], Array[Byte]](x, x) }
    "convert from String" in prop { (x: String) => checkConversion[String, Array[Byte]](x, x.getBytes) }
    "convert from null" in checkNull[Array[Byte]]
  }

  private def checkConversion[T, D](x: T, expected: => D)(implicit dt: DataType[D]): Boolean = {
    dt.convert(x) === expected
  }

  private def checkNull[D](implicit dt: DataType[D]): Boolean = {
    dt.convert(null) === null
  }

  private def shift(b: Byte, n: Int) = (b & 0xFFFFFFFF) << n
}