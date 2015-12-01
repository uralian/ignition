package com.ignition.frame

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL.{ jobject2assoc, pair2Assoc, pair2jvalue, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.MongoUtils
import com.ignition.util.XmlUtils.RichNodeSeq
import com.mongodb.casbah.commons.MongoDBObject

/**
 * Writes rows into a MongoDB collection.
 *
 * @author Vlad Orzhekhovskiy
 */
case class MongoOutput(db: String, coll: String) extends FrameTransformer {
  import MongoOutput._

  protected def compute(arg: DataFrame, preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    val db = this.db
    val coll = this.coll

    val df = optLimit(arg, preview)
    df foreachPartition { rows =>
      val collection = MongoUtils.collection(db, coll)
      rows foreach { row =>
        val data = row.schema zip row.toSeq map {
          case (field, value) => field.name -> value
        }
        val doc = MongoDBObject(data: _*)
        collection.save(doc)
      }
    }

    df
  }

  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType = input(true).schema

  def toXml: Elem = <node db={ db } coll={ coll }/>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("db" -> db) ~ ("coll" -> coll)
}

/**
 * Mongo Output companion object.
 */
object MongoOutput {
  val tag = "mongo-output"

  def fromXml(xml: Node) = apply(xml \ "@db" asString, xml \ "@coll" asString)

  def fromJson(json: JValue) = apply(json \ "db" asString, json \ "coll" asString)
}