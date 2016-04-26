package com.ignition.frame

import java.sql.DriverManager

import org.apache.spark.sql.SaveMode
import org.json4s.JsonDSL
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.ExecutionException
import com.ignition.types.{ RichStructType, boolean, fieldToRichStruct, int, string }

@RunWith(classOf[JUnitRunner])
class JdbcOutputSpec extends FrameFlowSpecification {
  sequential

  val url = "jdbc:h2:~/test"

  val schema = string("name") ~ int("age") ~ boolean("married")

  override def beforeAll = prepareData

  "JdbcOutput" should {
    "construct with helpers" in {
      val step = JdbcOutput() url "url" table "table" mode SaveMode.Ignore username "user" password "pwd" properties ("a" -> "b")
      step.connUrl === "url"
      step.table === "table"
      step.saveMode === SaveMode.Ignore
      step.username === Some("user")
      step.password === Some("pwd")
      step.properties === Map("a" -> "b")
    }
    "insert data into table" in {
      val grid = DataGrid(schema) rows (("john", 35, true), ("jane", 20, false))
      val step = JdbcOutput(url, "people", SaveMode.Append)
      grid --> step
      assertSchema(schema, step, 0)
      loadData === Set(("john", 35, true), ("jane", 20, false))
    }
    "append data to table" in {
      val grid = DataGrid(schema) rows (("jake", 42, true))
      val step = JdbcOutput(url, "people", SaveMode.Append)
      grid --> step
      assertSchema(schema, step, 0)
      loadData === Set(("john", 35, true), ("jane", 20, false), ("jake", 42, true))
    }
    "overwrite data in table" in {
      val grid = DataGrid(schema) rows (("jack", 27, false))
      val step = JdbcOutput(url, "people", SaveMode.Overwrite)
      grid --> step
      assertSchema(schema, step, 0)
      loadData === Set(("jack", 27, false))
    }
    "ignore new data" in {
      val grid = DataGrid(schema) rows (("jill", 16, false))
      val step = JdbcOutput(url, "people", SaveMode.Ignore)
      grid --> step
      assertSchema(schema, step, 0)
      loadData === Set(("jack", 27, false))
    }
    "fail on existing data" in {
      val grid = DataGrid(schema) rows (("jess", 99, true))
      val step = JdbcOutput(url, "people", SaveMode.ErrorIfExists)
      grid --> step
      step.output must throwA[ExecutionException]
    }
    "fail on bad credentials" in {
      val step = JdbcInput(url, "select * from customers") username "unknown"
      step.output must throwA[ExecutionException]
    }
    "fail on bad JDBC url" in {
      val step = JdbcInput("???", "select * from customers")
      step.output must throwA[ExecutionException]
    }
    "fail on bad table name" in {
      val step = JdbcInput(url, "unknown")
      step.output must throwA[ExecutionException]
    }
    "save to/load from xml" in {
      val step1 = JdbcOutput() url "url" table "table" username "user" mode SaveMode.Ignore
      step1.toXml must ==/(
        <jdbc-output mode="Ignore">
          <url>url</url>
          <table>table</table>
          <username>user</username>
        </jdbc-output>)
      JdbcOutput.fromXml(step1.toXml) === step1

      val step2 = JdbcOutput("url", "table") properties ("a" -> "b", "c" -> "d")
      step2.toXml must ==/(
        <jdbc-output mode="Append">
          <url>url</url>
          <table>table</table>
          <properties>
            <property name="a">b</property>
            <property name="c">d</property>
          </properties>
        </jdbc-output>)
      JdbcOutput.fromXml(step2.toXml) === step2
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val step1 = JdbcOutput() url "url" table "table" username "user" mode SaveMode.Overwrite
      step1.toJson === ("tag" -> "jdbc-output") ~ ("url" -> "url") ~ ("table" -> "table") ~
        ("username" -> "user") ~ ("password" -> jNone) ~ ("properties" -> jNone) ~ ("mode" -> "Overwrite")
      JdbcOutput.fromJson(step1.toJson) === step1

      val step2 = JdbcOutput("url", "table") properties ("a" -> "b", "c" -> "d")
      step2.toJson === ("tag" -> "jdbc-output") ~ ("url" -> "url") ~ ("table" -> "table") ~
        ("username" -> jNone) ~ ("password" -> jNone) ~ ("mode" -> "Append") ~ ("properties" -> List(
          ("name" -> "a") ~ ("value" -> "b"),
          ("name" -> "c") ~ ("value" -> "d")))
      JdbcOutput.fromJson(step2.toJson) === step2
    }
    "be unserializable" in assertUnserializable(JdbcOutput())
  }

  /**
   * Creates data tables.
   */
  private def prepareData() = {
    val conn = DriverManager.getConnection(url)
    val stmt = conn.createStatement
    stmt.execute("""CREATE TABLE IF NOT EXISTS people (
      | name VARCHAR PRIMARY KEY,
      | age INT,
      | married BOOLEAN
      | );""".stripMargin)
    stmt.execute("TRUNCATE TABLE people")
    conn.close
  }

  /**
   * Loads data for checking.
   */
  private def loadData() = {
    val conn = DriverManager.getConnection(url)
    val rs = conn.createStatement.executeQuery("SELECT * FROM people")
    val data = new Iterator[(String, Int, Boolean)] {
      def hasNext = rs.next
      def next = (rs.getString(1), rs.getInt(2), rs.getBoolean(3))
    } toSet

    conn.close
    data
  }
}