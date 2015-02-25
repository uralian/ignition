package com.ignition.samples

import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import com.eaio.uuid.UUID
import com.ignition.data._
import com.ignition.workflow.rdd.grid.{ AddChecksum, DigestAlgorithm, SelectValues }
import com.ignition.workflow.rdd.grid.input.DataGridInput

object SimpleFlow extends App {
  val log = LoggerFactory.getLogger(getClass)

  implicit val sc = new SparkContext("local[4]", "test")

  val meta = uuid("id") ~ string("name") ~ int("weight") ~ datetime("dob")

  val grid = DataGridInput(meta).
    addRow(new UUID, "john", 155, dob(1980, 5, 2)).
    addRow(new UUID, "jane", 190, dob(1982, 4, 25)).
    addRow(new UUID, "jake", 160, dob(1974, 11, 3)).
    addRow(new UUID, "josh", 120, dob(1995, 1, 10))

  val chksum = AddChecksum("chk", DigestAlgorithm.SHA256, List("name", "weight"))

  val select = SelectValues().retype[String]("dob").retain("id", "dob", "chk")

  grid.connectTo(chksum).connectTo(select)

  println("Output meta: " + select.outMetaData)
  println("Output data:")
  select.output foreach println

  sc.stop

  def dob(year: Int, month: Int, day: Int) = new DateTime(year, month, day, 0, 0)
}