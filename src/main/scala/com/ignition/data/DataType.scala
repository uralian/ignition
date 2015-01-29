package com.ignition.data

import java.nio.ByteBuffer
import scala.Array.canBuildFrom
import scala.math.BigDecimal
import scala.math.BigDecimal.{ double2bigDecimal, int2bigDecimal, long2bigDecimal }
import scala.reflect.runtime.universe._
import org.joda.time.DateTime
import com.eaio.uuid.UUID
import java.nio.ByteOrder

/**
 * An exception thrown when the data type conversion fails.
 */
class TypeConversionException(val message: String, cause: Exception = null) extends Exception(message, cause)

/**
 * Trait responsible for conversion Any data values to the specified data type.
 *
 * @author Vlad Orzhekhovskiy
 */
sealed trait DataType[T] {

  type targetType = T

  def targetTypeTag: TypeTag[T]

  def targetTypeName: String = targetTypeTag.tpe.toString

  def convertPF: PartialFunction[Any, T]

  def convert(obj: Any): T = {
    convertPF.applyOrElse(obj, (_: Any) =>
      if (obj != null)
        throw new TypeConversionException(s"Cannot convert object $obj of type ${obj.getClass} to $targetTypeName.")
      else
        throw new TypeConversionException(s"Cannot convert object $obj to $targetTypeName."))
  }
}

/**
 * Data type to handle possible null values.
 */
sealed trait NullableDataType[T] extends DataType[T] {
  override def convert(obj: Any): T =
    if (obj != null)
      super.convert(obj)
    else
      null.asInstanceOf[T]
}

/**
 * Supported data types:
 *
 * <ul>
 * 	<li>Boolean</li>
 * 	<li>String</li>
 * 	<li>Int</li>
 * 	<li>Double</li>
 * 	<li>BigDecimal</li>
 *  <li>DateTime</li>
 *  <li>UUID</li>
 *  <li>Array[Byte]</li>
 * </ul>
 */
object DataType {

  implicit def boolean2bytes(x: Boolean) = Array(if (x) 1.toByte else 0.toByte)
  implicit def int2bytes(x: Int) = ByteBuffer.allocate(4).putInt(x).array
  implicit def long2bytes(x: Long) = ByteBuffer.allocate(8).putLong(x).array
  implicit def double2bytes(x: Double) = ByteBuffer.allocate(8).putDouble(x).array

  def bytes2buffer(x: Array[Byte], size: Int): ByteBuffer =
    ByteBuffer.wrap(x.take(size).reverse.padTo(size, 0.toByte)).order(ByteOrder.LITTLE_ENDIAN)

  implicit object BooleanDataType extends NullableDataType[Boolean] {
    def targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[Boolean]]
    }
    def convertPF = {
      case x: Boolean => x
      case x: Number => x != 0
      case x: DateTime => x.getMillis != 0
      case x: UUID => x.hashCode != 0
      case x: Array[Byte] => x.exists(_ != 0)
      case x: String => x.toLowerCase == "true"
    }
  }

  implicit object StringDataType extends NullableDataType[String] {
    def targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[String]]
    }
    def convertPF = {
      case x: Array[Byte] => new String(x)
      case x => x.toString
    }
  }

  implicit object IntDataType extends NullableDataType[Int] {
    def targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[Int]]
    }
    def convertPF = {
      case x: Number => x.intValue
      case x: Boolean => if (x) 1 else 0
      case x: DateTime => x.getMillis.toInt
      case x: UUID => x.hashCode
      case x: Array[Byte] => bytes2buffer(x, 4).getInt
      case x: String => x.toInt
    }
  }

  implicit object DoubleDataType extends NullableDataType[Double] {
    def targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[Double]]
    }
    def convertPF = {
      case x: Number => x.doubleValue
      case x: Boolean => if (x) 1.0 else 0.0
      case x: DateTime => x.getMillis.toDouble
      case x: UUID => x.hashCode
      case x: Array[Byte] => bytes2buffer(x, 8).getDouble
      case x: String => x.toDouble
    }
  }

  implicit object DecimalDataType extends NullableDataType[scala.math.BigDecimal] {
    def targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[BigDecimal]]
    }
    def convertPF = {
      case x: BigDecimal => x
      case x: Number => BigDecimal(x.toString)
      case x: Boolean => if (x) 1 else 0
      case x: DateTime => x.getMillis
      case x: UUID => x.hashCode
      case x: Array[Byte] => bytes2buffer(x, 8).getDouble
      case x: String => BigDecimal(x)
    }
  }

  implicit object DateTimeDataType extends NullableDataType[DateTime] {
    def targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[DateTime]]
    }
    def convertPF = {
      case x: DateTime => x
      case x: Number => new DateTime(x.longValue)
      case x: UUID => new DateTime(x.getTime)
      case x: Array[Byte] => new DateTime(bytes2buffer(x, 8).getLong)
      case x: String => DateTime.parse(x)
    }
  }

  implicit object UUIDDataType extends NullableDataType[UUID] {
    def targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[UUID]]
    }
    def convertPF = {
      case x: UUID => x
      case x: String => new UUID(x)
    }
  }

  implicit object BinaryDataType extends NullableDataType[Array[Byte]] {
    def targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[Array[Byte]]]
    }
    def convertPF = {
      case x: Array[Byte] => x
      case x: Boolean => x: Array[Byte]
      case x: Int => x: Array[Byte]
      case x: Double => x: Array[Byte]
      case x: BigDecimal => x.bigDecimal.unscaledValue.toByteArray
      case x: DateTime => x.getMillis: Array[Byte]
      case x: UUID => (x.getClockSeqAndNode: Array[Byte]) ++ (x.getTime: Array[Byte])
      case x: String => x.getBytes
    }
  }
}