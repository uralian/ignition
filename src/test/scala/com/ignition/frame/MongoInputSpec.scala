package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.specification.Fragments
import org.specs2.runner.JUnitRunner

import com.github.athieriot.EmbedConnection
import com.ignition.types.{ RichStructType, boolean, fieldToRichStruct, string }
import com.ignition.util.MongoUtils
import com.mongodb.casbah.Imports.MongoDBObject

@RunWith(classOf[JUnitRunner])
class MongoInputSpec extends FrameFlowSpecification with EmbedConnection {
  sequential

  override def beforeAll = mongodExecutable.start

  override def afterAll = mongodExecutable.stop

  override def map(fs: => Fragments) = super[FrameFlowSpecification].map(fs)

  "MongoInput" should {
    "load data without filtering" in {
      insertAccounts(3)
      val schema = string("code", false) ~ string("owner#name") ~ boolean("active")
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
      val schema = string("code", false) ~ string("type") ~ string("owner#address#zip")
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
      val schema = string("code", false) ~ string("owner#address#zip", false)
      val mongo = MongoInput("test", "accounts", schema)
      mongo.output.collect must throwA[RuntimeException]
    }
    "save to/load from xml" in {
      val schema = string("code", false) ~ string("owner#name") ~ boolean("active")
      val m1 = MongoInput("test", "accounts", schema)
      m1.toXml must ==/(
        <mongo-input db="test" coll="accounts">
          <schema>
            <field name="code" type="string" nullable="false"/>
            <field name="owner#name" type="string" nullable="true"/>
            <field name="active" type="boolean" nullable="true"/>
          </schema>
          <page limit="100" offset="0"/>
        </mongo-input>)
      MongoInput.fromXml(m1.toXml) === m1

      val m2 = MongoInput("test", "accounts", schema) where ("code" -> "123",
        "active" -> true) orderBy ("owner#name" -> true, "code" -> false) limit (0) offset (0)
      m2.toXml must ==/(
        <mongo-input db="test" coll="accounts">
          <schema>
            <field name="code" type="string" nullable="false"/>
            <field name="owner#name" type="string" nullable="true"/>
            <field name="active" type="boolean" nullable="true"/>
          </schema>
          <filter>
            <field name="code" type="string">123</field>
            <field name="active" type="boolean">true</field>
          </filter>
          <sort-by>
            <field name="owner#name" direction="asc"/>
            <field name="code" direction="desc"/>
          </sort-by>
        </mongo-input>)
      MongoInput.fromXml(m2.toXml) === m2
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val schema = string("code", false) ~ string("owner#name") ~ boolean("active")
      val m1 = MongoInput("test", "accounts", schema)
      m1.toJson === ("tag" -> "mongo-input") ~ ("db" -> "test") ~ ("coll" -> "accounts") ~
        ("schema" -> List(
          ("name" -> "code") ~ ("type" -> "string") ~ ("nullable" -> false),
          ("name" -> "owner#name") ~ ("type" -> "string") ~ ("nullable" -> true),
          ("name" -> "active") ~ ("type" -> "boolean") ~ ("nullable" -> true))) ~
          ("filter" -> jNone) ~
          ("sortBy" -> jNone) ~
          ("page" -> ("limit" -> 100) ~ ("offset" -> 0))
      MongoInput.fromJson(m1.toJson) === m1

      val m2 = MongoInput("test", "accounts", schema) where ("code" -> "123",
        "active" -> true) orderBy ("owner#name" -> true, "code" -> false) limit (0) offset (0)
      m2.toJson === ("tag" -> "mongo-input") ~ ("db" -> "test") ~ ("coll" -> "accounts") ~
        ("schema" -> List(
          ("name" -> "code") ~ ("type" -> "string") ~ ("nullable" -> false),
          ("name" -> "owner#name") ~ ("type" -> "string") ~ ("nullable" -> true),
          ("name" -> "active") ~ ("type" -> "boolean") ~ ("nullable" -> true))) ~
          ("filter" -> List(
            ("name" -> "code") ~ ("type" -> "string") ~ ("value" -> "123"),
            ("name" -> "active") ~ ("type" -> "boolean") ~ ("value" -> true))) ~
            ("sortBy" -> List(
              ("name" -> "owner#name") ~ ("direction" -> "asc"),
              ("name" -> "code") ~ ("direction" -> "desc"))) ~
              ("page" -> jNone)
      MongoInput.fromJson(m2.toJson) === m2
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