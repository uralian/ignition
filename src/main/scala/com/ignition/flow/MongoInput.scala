package com.ignition.flow

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.types._

import com.ignition.types.RichBoolean
import com.ignition.util.MongoUtils
import com.ignition.util.MongoUtils.DBObjectHelper
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._

/**
 * Used to limit the results returned from data store queries.
 */
case class Page(limit: Int = 100, offset: Int = 0)
object Page {
  lazy val default = new Page
  lazy val unbounded = new Page(0, 0)
}

/**
 * A sorting order.
 */
case class SortOrder(field: String, ascending: Boolean = true)

/**
 * Reads documents from MongoDB.
 *
 * @author Vlad Orzhekhovskiy
 */
case class MongoInput(db: String, coll: String, schema: StructType,
  filter: Map[String, Any] = Map.empty, sort: Iterable[SortOrder] = List.empty,
  page: Page = Page.default) extends Producer {

  import MongoUtils._

  def where(filter: (String, Any)*) = copy(filter = filter.toMap)
  def sort(sort: Iterable[SortOrder]) = copy(sort = sort)
  def orderBy(field: String, ascending: Boolean) = copy(sort = sort.toSeq :+ SortOrder(field, ascending))
  def limit(limit: Int) = copy(page = Page(limit, this.page.offset))
  def offset(offset: Int) = copy(page = Page(this.page.limit, offset))

  protected def compute(limit: Option[Int])(implicit ctx: SQLContext): DataFrame = {
    val collection = MongoUtils.collection(db, coll)

    val fields = schema map (f => f.copy(name = f.name.replace('#', '.')))

    val keys: Map[String, Boolean] = Map("_id" -> false) ++ fields.map(_.name -> true)
    val query = filterToDBObject(filter)
    val orderBy = sortToDBObject(sort)

    val cursor = collection.find(query, keys).sort(orderBy)
    val cursorWithOffset = if (page.offset > 0) cursor.skip(page.offset) else cursor
    val size = limit map { n =>
      if (page.limit > 0) math.min(n, page.limit) else n
    } orElse {
      if (page.limit > 0) Some(page.limit) else None
    }
    val cursorWithLimit = size map cursorWithOffset.limit getOrElse cursorWithOffset

    val rows = cursorWithLimit map { obj =>
      val data = fields map { field =>
        val value = extract(obj, field.name, field.dataType)
        value orElse {
          field.nullable option null
        } getOrElse {
          throw new IllegalArgumentException(s"Mandatory field is null: ${field.name}")
        }
      }
      Row.fromSeq(data)
    }
    val rdd = ctx.sparkContext.parallelize(rows.toSeq)
    ctx.createDataFrame(rdd, schema)
  }

  protected def computeSchema(implicit ctx: SQLContext): StructType = schema

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable

  private def filterToDBObject(filter: Map[String, Any]): DBObject = filter map {
    case (key, value: List[Any]) => key -> MongoDBList(value: _*)
    case (key, value: Map[_, _]) => key -> filterToDBObject(value.asInstanceOf[Map[String, Any]])
    case (key, value) => key -> value
  }

  private def sortToDBObject(sort: Iterable[SortOrder]) = MongoDBObject(sort.map {
    case SortOrder(field, ascending) => (field -> (if (ascending) 1 else -1))
  } toList)

  /**
   * Extracts a value by a specified name, which can be either a simple key or a path
   * in the format name1.name2...nameN, in which case the function navigates the subdocuments
   * till the last segment.
   */
  private def extract(doc: DBObject, name: String, dataType: DataType): Option[Any] =
    name.split('.').toList match {
      case Nil => None
      case head :: Nil => getValue(doc, head, dataType)
      case head :: tail => doc getAsDBObject head flatMap { subdoc =>
        val subname = name.drop(head.length + 1)
        extract(subdoc, subname, dataType)
      }
    }

  /**
   * Extracts a value by the specified key, using the supplied data type.
   * It returns Some(x) or None, if the key does not exist.
   */
  private def getValue(doc: DBObject, key: String, dataType: DataType) = dataType match {
    case BinaryType => throw new IllegalArgumentException(s"BinaryType not supported for Mongo")
    case BooleanType => doc getAsBoolean key
    case StringType => doc getAsString key
    case ByteType => doc getAsNumber key map (_.byteValue)
    case ShortType => doc getAsNumber key map (_.shortValue)
    case IntegerType => doc getAsNumber key map (_.intValue)
    case LongType => doc getAsNumber key map (_.longValue)
    case FloatType => doc getAsDouble key map (_.toFloat)
    case DoubleType => doc getAsDouble key
    case _: DecimalType => doc getAsDouble key map Decimal.apply
    case DateType => doc.getAs[java.util.Date](key) map (x => new java.sql.Date(x.getTime))
    case TimestampType => doc.getAs[java.util.Date](key) map (x => new java.sql.Timestamp(x.getTime))
    case dt => throw new IllegalArgumentException(s"Invalid data type: $dt")
  }
}