package com.ignition.frame

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime
import com.ignition.util.MongoUtils
import com.mongodb.casbah.commons.MongoDBObject

/**
 * Writes rows into a MongoDB collection.
 *
 * @author Vlad Orzhekhovskiy
 */
case class MongoOutput(db: String, coll: String) extends FrameTransformer {

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val db = this.db
    val coll = this.coll

    val df = optLimit(arg, limit)
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

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType = inSchema
}