package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.specification.Fragments
import org.specs2.runner.JUnitRunner

import com.github.athieriot.EmbedConnection
import com.ignition.types.{ RichStructType, boolean, fieldToRichStruct, int, string }
import com.ignition.util.MongoUtils
import com.mongodb.casbah.Imports.{ MongoDBObject, map2MongoDBObject }

@RunWith(classOf[JUnitRunner])
class MongoOutputSpec extends FrameFlowSpecification with EmbedConnection {
  sequential

  override def beforeAll = {
    super.beforeAll
    mongodExecutable.start
  }

  override def afterAll = {
    mongodExecutable.stop
    super.afterAll
  }

  override def map(fs: => Fragments) = super[FrameFlowSpecification].map(fs)

  "MongoOutput" should {
    "save data without nulls to MongoDB" in {
      val coll = MongoUtils.collection("test", "accounts")
      coll.remove(MongoDBObject.empty)
      val schema = string("code", false) ~ string("name") ~ boolean("active")
      val grid = DataGrid(schema).addRow("111", "john", true).addRow("222", "jake", false)
      val mongo = MongoOutput("test", "accounts")
      grid --> mongo
      mongo.output
      coll.find(MongoDBObject.empty, Map("_id" -> false)).toSet === Set(
        MongoDBObject("code" -> "111", "name" -> "john", "active" -> true),
        MongoDBObject("code" -> "222", "name" -> "jake", "active" -> false))
    }
    "save data with nulls to MongoDB" in {
      val coll = MongoUtils.collection("test", "accounts")
      coll.remove(MongoDBObject.empty)
      val schema = int("code", false) ~ string("name") ~ boolean("active")
      val grid = DataGrid(schema).addRow(111, "john", null).addRow(222, null, false)
      val mongo = MongoOutput("test", "accounts")
      grid --> mongo
      mongo.output
      coll.find(MongoDBObject.empty, Map("_id" -> false)).toSet === Set(
        MongoDBObject("code" -> 111, "name" -> "john", "active" -> null),
        MongoDBObject("code" -> 222, "name" -> null, "active" -> false))
    }
    "save to/load from xml" in {
      val mongo = MongoOutput("test", "accounts")
      mongo.toXml must ==/(<mongo-output db="test" coll="accounts" />)
      MongoOutput.fromXml(mongo.toXml) === mongo
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._
      
      val mongo = MongoOutput("test", "accounts")
      mongo.toJson === ("tag" -> "mongo-output") ~ ("db" -> "test") ~ ("coll" -> "accounts")
      MongoOutput.fromJson(mongo.toJson) === mongo
    }
    "be unserializable" in assertUnserializable(MongoOutput("test", "accounts"))
  }
}