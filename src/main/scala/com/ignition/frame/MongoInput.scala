package com.ignition.frame

import scala.xml.{ Elem, Node }
import scala.xml.NodeSeq.seqToNodeSeq

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types._
import org.json4s.{ JObject, JValue }
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic

import com.ignition.types.RichBoolean
import com.ignition.types.TypeUtils._
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.MongoUtils
import com.ignition.util.XmlUtils.{ RichNodeSeq, intToText }
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
                      page: Page = Page.default) extends FrameProducer {

  import MongoInput._
  import MongoUtils._

  def where(filter: (String, Any)*) = copy(filter = filter.toMap)
  def orderBy(tuples: (String, Boolean)*) = copy(sort = tuples.map(t => SortOrder(t._1, t._2)))
  def limit(limit: Int) = copy(page = Page(limit, this.page.offset))
  def offset(offset: Int) = copy(page = Page(this.page.limit, offset))

  protected def compute(preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    val collection = MongoUtils.collection(db, coll)

    val fields = schema map (f => f.copy(name = f.name.replace('#', '.')))

    val keys: Map[String, Boolean] = Map("_id" -> false) ++ fields.map(_.name -> true)
    val query = filterToDBObject(filter)
    val orderBy = sortToDBObject(sort)

    val cursor = collection.find(query, keys).sort(orderBy)
    val cursorWithOffset = if (page.offset > 0) cursor.skip(page.offset) else cursor
    val size = (page.limit, preview) match {
      case (limit, false) if limit < 1 => None
      case (limit, true) if limit < 1 => Some(FrameStep.previewSize)
      case (limit, false) => Some(limit)
      case (limit, true) => Some(math.min(FrameStep.previewSize, limit))
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

  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType = schema

  def toXml: Elem =
    <node db={ db } coll={ coll }>
      { DataGrid.schemaToXml(schema) }
      {
        if (!filter.isEmpty)
          <filter>
            {
              filter map {
                case (name, value) =>
                  <field name={ name } type={ nameForType(typeForValue(value)) }>{ valueToXml(value) }</field>
              }
            }
          </filter>
      }
      {
        if (!sort.isEmpty)
          <sort-by>
            {
              sort map (so => <field name={ so.field } direction={ so.ascending ? ("asc", "desc") }/>)
            }
          </sort-by>
      }
      {
        if (page != Page.unbounded)
          <page limit={ page.limit } offset={ page.offset }/>
      }
    </node>.copy(label = tag)

  def toJson: JValue = {
    val filterJson = if (filter.isEmpty) None else Some(filter.map {
      case (name, value) => ("name" -> name) ~ ("type" -> nameForType(typeForValue(value))) ~ ("value" -> valueToJson(value))
    })
    val sortJson = if (sort.isEmpty) None else Some(sort.map {
      case SortOrder(name, asc) => JObject("name" -> name, "direction" -> asc ? ("asc", "desc"))
    })
    val pageJson = if (page == Page.unbounded) None else Some(
      ("limit" -> page.limit) ~ ("offset" -> page.offset))

    ("tag" -> tag) ~ ("db" -> db) ~ ("coll" -> coll) ~
      ("schema" -> DataGrid.schemaToJson(schema)) ~
      ("filter" -> filterJson) ~
      ("sortBy" -> sortJson) ~
      ("page" -> pageJson)
  }

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

/**
 * Mongo Input companion object.
 */
object MongoInput {
  val tag = "mongo-input"

  def fromXml(xml: Node) = {
    val db = xml \ "@db" asString
    val coll = xml \ "@coll" asString
    val schema = DataGrid.xmlToSchema((xml \ "schema").head)
    val filter = xml \ "filter" \ "field" map { node =>
      val name = node \ "@name" asString
      val dataType = typeForName(node \ "@type" asString)
      val value = xmlToValue(dataType, node.child)
      name -> value
    } toMap
    val sort = xml \ "sort-by" \ "field" map { node =>
      val name = node \ "@name" asString
      val asc = (node \ "@direction" asString).toLowerCase startsWith "asc"
      SortOrder(name, asc)
    }
    val page = (xml \ "page").headOption map { node =>
      val limit = node \ "@limit" asInt
      val offset = node \ "@offset" asInt

      Page(limit, offset)
    } getOrElse Page.unbounded
    apply(db, coll, schema, filter, sort, page)
  }

  def fromJson(json: JValue) = {
    val db = json \ "db" asString
    val coll = json \ "coll" asString
    val schema = DataGrid.jsonToSchema(json \ "schema")
    val filter = (json \ "filter" asArray) map { node =>
      val name = node \ "name" asString
      val dataType = typeForName(node \ "type" asString)
      val value = jsonToValue(dataType, node \ "value")
      name -> value
    } toMap
    val sort = (json \ "sortBy" asArray) map { node =>
      val name = node \ "name" asString
      val asc = (node \ "direction" asString).toLowerCase startsWith "asc"
      SortOrder(name, asc)
    }
    val limit = json \ "page" \ "limit" getAsInt
    val offset = json \ "page" \ "offset" getAsInt
    val page = (for (l <- limit; o <- offset) yield Page(l, o)) getOrElse Page.unbounded
    apply(db, coll, schema, filter, sort, page)
  }
}