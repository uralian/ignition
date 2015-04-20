package com.ignition.flow

import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.types.StructType
import com.ignition.util.MongoUtils
import com.mongodb.casbah.commons.MongoDBObject

/**
 * Writes rows into a MongoDB collection.
 *
 * @author Vlad Orzhekhovskiy
 */
case class MongoOutput(db: String, coll: String) extends Transformer {

  protected def compute(arg: DataFrame)(implicit ctx: SQLContext): DataFrame = {
    val db = this.db
    val coll = this.coll

    arg foreachPartition { rows =>
      val collection = MongoUtils.collection(db, coll)
      rows foreach { row =>
        val data = row.schema zip row.toSeq map {
          case (field, value) => field.name -> value
        }
        val doc = MongoDBObject(data: _*)
        collection.save(doc)
      }
    }
    
    arg
  }

  protected def computeSchema(inSchema: Option[StructType])(implicit ctx: SQLContext): Option[StructType] =
    inSchema

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}