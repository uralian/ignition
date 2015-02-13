package com.ignition.data

import java.nio.ByteBuffer
import scala.Array.canBuildFrom
import scala.math.BigDecimal
import scala.math.BigDecimal.{ double2bigDecimal, int2bigDecimal, long2bigDecimal }
import scala.reflect.runtime.universe._
import org.joda.time.DateTime
import com.eaio.uuid.UUID
import java.nio.ByteOrder
import scala.util.control.NonFatal

/**
 * An exception thrown when the data type conversion fails.
 */
class TypeConversionException(val message: String, cause: Throwable = null) extends Exception(message, cause)

/**
 * Trait responsible for conversion Any data values to the specified data type.
 *
 * @author Vlad Orzhekhovskiy
 */
sealed trait DataType[T] extends Serializable {

  type targetType = T

  def targetTypeTag: TypeTag[T]

  def targetTypeName: String = targetTypeTag.tpe.toString

  def code: String

  protected def convertPF: PartialFunction[Any, T]

  def convert(obj: Any): T = try {
    convertPF applyOrElse (obj, (_: Any) => obj match {
      case null => throw new TypeConversionException(s"Cannot convert null to $targetTypeName.")
      case obj => throw new TypeConversionException(s"Cannot convert object $obj of type ${obj.getClass.getSimpleName} to $targetTypeName.")
    })
  } catch {
    case e: TypeConversionException => throw e
    case NonFatal(e) => throw new TypeConversionException(e.getMessage, e)
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

  def withCode(code: String): DataType[_] = code.toLowerCase match {
    case BooleanDataType.code => BooleanDataType
    case StringDataType.code => StringDataType
    case IntDataType.code => IntDataType
    case DoubleDataType.code => DoubleDataType
    case DecimalDataType.code => DecimalDataType
    case DateTimeDataType.code => DateTimeDataType
    case UUIDDataType.code => UUIDDataType
    case BinaryDataType.code => BinaryDataType
  }

  implicit object BooleanDataType extends NullableDataType[Boolean] {
    def targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[Boolean]]
    }
    val code = "boolean"
    protected def convertPF = {
      case x: Boolean => x
      case x: Number => x != 0
      case x: DateTime => x.getMillis != 0
      case x: UUID => x.hashCode != 0
      case x: Binary => x.exists(_ != 0)
      case x: String => x.toLowerCase == "true"
    }
  }

  implicit object StringDataType extends NullableDataType[String] {
    def targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[String]]
    }
    val code = "string"
    protected def convertPF = {
      case x: Binary => new String(x)
      case x => x.toString
    }
  }

  implicit object IntDataType extends NullableDataType[Int] {
    def targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[Int]]
    }
    val code = "int"
    protected def convertPF = {
      case x: Number => x.intValue
      case x: Boolean => if (x) 1 else 0
      case x: DateTime => x.getMillis.toInt
      case x: UUID => x.hashCode
      case x: Binary => bytes2buffer(x, 4).getInt
      case x: String => x.toInt
    }
  }

  implicit object DoubleDataType extends NullableDataType[Double] {
    def targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[Double]]
    }
    val code = "double"
    protected def convertPF = {
      case x: Number => x.doubleValue
      case x: Boolean => if (x) 1.0 else 0.0
      case x: DateTime => x.getMillis.toDouble
      case x: UUID => x.hashCode
      case x: Binary => bytes2buffer(x, 8).getDouble
      case x: String => x.toDouble
    }
  }

  implicit object DecimalDataType extends NullableDataType[Decimal] {
    def targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[Decimal]]
    }
    val code = "decimal"
    protected def convertPF = {
      case x: Decimal => x
      case x: Number => BigDecimal(x.toString)
      case x: Boolean => if (x) 1 else 0
      case x: DateTime => x.getMillis
      case x: UUID => x.hashCode
      case x: Binary => bytes2buffer(x, 8).getDouble
      case x: String => BigDecimal(x)
    }
  }

  implicit object DateTimeDataType extends NullableDataType[DateTime] {
    def targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[DateTime]]
    }
    val code = "datetime"
    protected def convertPF = {
      case x: DateTime => x
      case x: Number => new DateTime(x.longValue)
      case x: UUID => new DateTime(x.getTime)
      case x: Binary => new DateTime(bytes2buffer(x, 8).getLong)
      case x: String => DateTime.parse(x)
    }
  }

  implicit object UUIDDataType extends NullableDataType[UUID] {
    def targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[UUID]]
    }
    val code = "uuid"
    protected def convertPF = {
      case x: UUID => x
      case x: String => new UUID(x)
    }
  }

  implicit object BinaryDataType extends NullableDataType[Binary] {
    def targetTypeTag = TypeTag.synchronized {
      implicitly[TypeTag[Binary]]
    }
    val code = "binary"
    protected def convertPF = {
      case x: Binary => x
      case x: Boolean => x: Binary
      case x: Int => x: Binary
      case x: Double => x: Binary
      case x: Decimal => x.bigDecimal.unscaledValue.toByteArray
      case x: DateTime => x.getMillis: Binary
      case x: UUID => (x.getClockSeqAndNode: Binary) ++ (x.getTime: Binary)
      case x: String => x.getBytes
    }
  }
}