package com.ignition.flow

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.github.athieriot.EmbedConnection
import com.ignition.types._
import com.ignition.util.MongoUtils
import com.mongodb.casbah.Imports.MongoDBObject

@RunWith(classOf[JUnitRunner])
class MongoInputSpec extends FlowSpecification with EmbedConnection {
  sequential

  "MongoInput" should {
    "load data without filtering" in {
      insertAccounts(3)
      val schema = string("code", false) ~ string("owner.name") ~ boolean("active")
      val mongo = MongoInput("test", "accounts", schema)
      assertOutput(mongo, 0, Seq("1", "John 1", true), Seq("2", "John 2", true), Seq("3", "John 3", false))
    }
    "load data with filtering" in {
      insertAccounts(10)
      val schema = string("code", false).schema
      val mongo = MongoInput("test", "accounts", schema,
        Map("$or" -> List(
          "balance" -> Map("$lt" -> 8),
          "balance" -> Map("$gt" -> 20))))
      assertOutput(mongo, 0, Seq("1"), Seq("2"), Seq("3"), Seq("10"))
    }
    "load data with nulls" in {
      insertAccounts(10)
      val schema = string("code", false) ~ string("type") ~ string("owner.address.zip")
      val mongo = MongoInput("test", "accounts", schema)
      assertOutput(mongo, 0,
        Seq("1", "checking", "00100"),
        Seq("2", "checking", "00200"),
        Seq("3", "checking", null),
        Seq("4", "checking", "00400"),
        Seq("5", null, "00500"),
        Seq("6", "checking", "00600"),
        Seq("7", "checking", "00700"),
        Seq("8", "checking", "00800"),
        Seq("9", "checking", "00900"),
        Seq("10", "checking", "01000"))
    }
    "fail for nulls in non-nullable fields" in {
      insertAccounts(10)
      val schema = string("code", false) ~ string("owner.address.zip", false)
      val mongo = MongoInput("test", "accounts", schema)
      mongo.output.collect must throwA[RuntimeException]
    }
    "be unserializable" in assertUnserializable(MongoInput("test", "accounts", string("code", false).schema))
  }

  private def insertAccounts(count: Int) = {
    val accounts = MongoUtils.collection("test", "accounts")
    accounts.remove(MongoDBObject.empty)
    (1 to count) foreach { index =>
      val acctType = if (index == 5) null else "checking"
      val address = if (index == 3) null else MongoDBObject("city" -> "Atlanta", "zip" -> f"${index * 100}%05d")
      val balance = if (index == 7) null else index * 2.2
      val doc = MongoDBObject("code" -> s"$index", "active" -> ((index % 3) != 0),
        "type" -> acctType, "balance" -> balance,
        "owner" -> MongoDBObject("name" -> s"John $index", "address" -> address))
      accounts.save(doc)
    }
  }
}