package com.ignition.samples

import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import com.eaio.uuid.UUID
import com.ignition.data._
import com.ignition.workflow.rdd.grid._
import com.ignition.workflow.rdd.grid.input._

object UnionFlow extends App {

  val log = LoggerFactory.getLogger(getClass)

  implicit val sc = new SparkContext("local[4]", "test")

  val meta = uuid("id") ~ string("name") ~ int("weight") ~ datetime("dob")
    
  val grid1 = DataGridInput(meta).
    addRow(new UUID, "john", 155, dob(1980, 5, 2)).
    addRow(new UUID, "jake", 160, dob(1974, 11, 3)).
    addRow(new UUID, "josh", 120, dob(1995, 1, 10))
    
  val grid2 = DataGridInput(meta).
    addRow(new UUID, "jane", 190, dob(1982, 4, 25)).
    addRow(new UUID, "jake", 160, dob(1974, 11, 3)).
    addRow(new UUID, "jill", 120, dob(1995, 1, 10))

  val grid3 = DataGridInput(meta).
    addRow(new UUID, "jess", 155, dob(1980, 5, 2))
    
  val union = Union()
  union.connectFrom(grid1)
  union.connectFrom(grid2)
  union.connectFrom(grid3)

  println("Output meta: " + union.outMetaData)
  println("Output data:")
  union.output foreach println
  
  sc.stop
  
  def dob(year: Int, month: Int, day: Int) = new DateTime(year, month, day, 0, 0)
}