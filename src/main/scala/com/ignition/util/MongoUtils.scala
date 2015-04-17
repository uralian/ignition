package com.ignition.util

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.conversions.scala._

/**
 * Utilities for interacting with MongoDB.
 *
 * @author Vlad Orzhekhovskiy
 */
object MongoUtils {

  RegisterConversionHelpers()

  val mongoClient = {
    val uri = ConfigUtils.getConfig("mongo").getString("uri")
    val clientUri = MongoClientURI(uri)
    MongoClient(clientUri)
  }

  def database(name: String) = mongoClient(name)

  def collection(dbName: String, coll: String) = database(dbName)(coll)

  /**
   * Helper to facilitate data retriving from a mongo DBObject.
   */
  implicit class DBObjectHelper(val underlying: DBObject) {
    def asString(key: String): String = underlying.as[Object](key).toString

    def getAsString(key: String) = underlying.getAs[String](key)
    
    def asNumber(key: String) = underlying.as[Number](key)
    
    def getAsNumber(key: String) = underlying.getAs[Number](key)

    def asDouble(key: String) = underlying.as[Double](key)

    def getAsDouble(key: String) = underlying.getAs[Double](key)

    def asInt(key: String) = underlying.as[Int](key)

    def getAsInt(key: String) = underlying.getAs[Int](key)

    def asLong(key: String) = underlying.as[Long](key)

    def getAsLong(key: String) = underlying.getAs[Long](key)
    
    def asBoolean(key: String) = underlying.as[Boolean](key)

    def getAsBoolean(key: String) = underlying.getAs[Boolean](key)

    def asDBObject(key: String) = underlying.as[DBObject](key)

    def getAsDBObject(key: String) = underlying.getAs[DBObject](key)
  }
}