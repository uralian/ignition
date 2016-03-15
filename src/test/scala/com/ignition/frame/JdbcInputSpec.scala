package com.ignition.frame

import java.io.InputStreamReader
import java.sql.{ Connection, DriverManager }

import org.h2.tools.RunScript
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.ExecutionException
import com.ignition.types.{ RichStructType, binary, date, double, fieldToRichStruct, string }

@RunWith(classOf[JUnitRunner])
class JdbcInputSpec extends FrameFlowSpecification {
  sequential

  val url = "jdbc:h2:~/test"

  override def beforeAll = {
    val conn = DriverManager.getConnection(url)
    prepareData(conn)
    conn.close
  }

  "JdbcInput" should {
    "construct with helpers" in {
      val step = JdbcInput() url "url" sql "sql" username "user" password "pwd" properties ("a" -> "b")
      step.connUrl === "url"
      step.sql === "sql"
      step.username === Some("user")
      step.password === Some("pwd")
      step.properties === Map("a" -> "b")
    }
    "load data from a single table" in {
      val step = JdbcInput(url, "select * from customers")
      assertSchema(binary("CUSTOMER_ID", false) ~ string("NAME") ~ string("ADDRESS"), step, 0)
      assertOutput(step, 0,
        ("c7b44500-b6bf-11e4-a71e-12e3f512a338", "Apple", "Cupertino, CA"),
        ("de305d54-75b4-431b-adb2-eb6b9e546014", "Walmart", null))
    }
    "load data from multiple tables" in {
      val sql = """select o.order_id, c.customer_id, o.date, o.description, o.weight 
        | from orders o join customers c on o.customer_id=c.customer_id
        | where o.shipped=true""".stripMargin
      val step = JdbcInput(url, sql)
      assertSchema(binary("ORDER_ID", false) ~ binary("CUSTOMER_ID", false) ~
        date("DATE") ~ string("DESCRIPTION") ~ double("WEIGHT"), step, 0)
      assertOutput(step, 0,
        ("f3b3d990-ea26-11e5-9ce9-5e5517507c66", "c7b44500-b6bf-11e4-a71e-12e3f512a338",
          javaDate(2015, 2, 10), "electronics", 45.5),
        ("f3b3e2e6-ea26-11e5-9ce9-5e5517507c66", "de305d54-75b4-431b-adb2-eb6b9e546014",
          javaDate(2015, 1, 10), "groceries", 15.0),
        ("f3b3e8cc-ea26-11e5-9ce9-5e5517507c66", "de305d54-75b4-431b-adb2-eb6b9e546014",
          javaDate(2015, 1, 5), "furniture", 650.0))
    }
    "fail on bad credentials" in {
      val step = JdbcInput(url, "select * from customers") username "unknown"
      step.output must throwA[ExecutionException]
    }
    "fail on bad JDBC url" in {
      val step = JdbcInput("???", "select * from customers")
      step.output must throwA[ExecutionException]
    }
    "fail on bad SQL query" in {
      val step = JdbcInput(url, "select * from unknown")
      step.output must throwA[ExecutionException]
    }
    "save to/load from xml" in {
      val step1 = JdbcInput() url "url" sql "sql" username "user"
      step1.toXml must ==/(
        <jdbc-input>
          <url>url</url>
          <sql>sql</sql>
          <username>user</username>
        </jdbc-input>)
      JdbcInput.fromXml(step1.toXml) === step1

      val step2 = JdbcInput("url", "sql") properties ("a" -> "b", "c" -> "d")
      step2.toXml must ==/(
        <jdbc-input>
          <url>url</url>
          <sql>sql</sql>
          <properties>
            <property name="a">b</property>
            <property name="c">d</property>
          </properties>
        </jdbc-input>)
      JdbcInput.fromXml(step2.toXml) === step2
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val step1 = JdbcInput() url "url" sql "sql" username "user"
      step1.toJson === ("tag" -> "jdbc-input") ~ ("url" -> "url") ~ ("sql" -> "sql") ~
        ("username" -> "user") ~ ("password" -> jNone) ~ ("properties" -> jNone)
      JdbcInput.fromJson(step1.toJson) === step1

      val step2 = JdbcInput("url", "sql") properties ("a" -> "b", "c" -> "d")
      step2.toJson === ("tag" -> "jdbc-input") ~ ("url" -> "url") ~ ("sql" -> "sql") ~
        ("username" -> jNone) ~ ("password" -> jNone) ~ ("properties" -> List(
          ("name" -> "a") ~ ("value" -> "b"),
          ("name" -> "c") ~ ("value" -> "d")))
      JdbcInput.fromJson(step2.toJson) === step2
    }
  }

  /**
   * Creates tables and inserts data.
   */
  private def prepareData(conn: Connection) = {
    val reader = new InputStreamReader(getClass.getResourceAsStream("/h2.sql"))
    RunScript.execute(conn, reader)
  }
}