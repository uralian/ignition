package com.ignition

import scala.concurrent.blocking

import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Spark test helper.
 *
 * @author Vlad Orzhekhovskiy
 */
trait SparkTestHelper extends BeforeAllAfterAll {

  protected def sparkConf = new SparkConf(true).
    set("spark.cassandra.connection.host", CassandraBaseTestHelper.host).
    set("spark.cassandra.connection.native.port", CassandraBaseTestHelper.port.toString).
    set("spark.cassandra.connection.rpc.port", CassandraBaseTestHelper.thriftPort.toString)

  implicit protected val sc = new SparkContext("local[4]", "test", sparkConf)

  protected def clearContext = blocking {
    sc.stop
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.master.port")
  }

  override def afterAll = clearContext
}