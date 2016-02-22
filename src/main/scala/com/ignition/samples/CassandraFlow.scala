package com.ignition.samples

import java.util.UUID

import scala.util.Random

import com.datastax.driver.core.{ Cluster, Session }
import com.ignition.{ ExecutionException, SparkHelper, frame }
import com.ignition.frame.{ DefaultSparkRuntime, FrameStepListener }
import com.ignition.frame.ReduceOp.{ MAX, MIN }

/**
 * Reads Cassandra data and manipulates it. Demonstrates using the step cache.
 */
object CassandraFlow extends App {

  if (args.length < 2) {
    Console.err.println(s"""
        |Usage: CassandraFlow host:port keyspace
        |  <host:port> host and port for a Cassandra node
        |  <keyspace> keyspace to use
        """.stripMargin)
    sys.exit(1)
  }

  val Array(url, keyspace) = args
  val Array(host, port) = url.split(":")

  System.setProperty("ignition.cassandra.host", host)
  System.setProperty("ignition.cassandra.port", port)

  val cluster = Cluster.builder.addContactPoint(host).withPort(port.toInt).build
  val tbl = "ign_data"

  {
    setUp

    implicit val rt = new DefaultSparkRuntime(SparkHelper.sqlContext)

    val listener = new FrameStepListener {
      override def onAfterStepComputed(event: frame.AfterFrameStepComputed) = {
        println(event.step + "[" + event.index + "]:")
        event.value.show
      }
    }

    val cassIn = frame.CassandraInput(keyspace, tbl)
    cassIn.addStepListener(listener)

    val filter = frame.Filter("name = 'john'")
    filter.addStepListener(listener)

    cassIn --> filter
    filter.evaluate

    val sql = frame.SQLQuery("select name, avg(score) as avg_score from input0 group by name")
    sql.addStepListener(listener)

    filter.out(0) --> sql
    sql.evaluate

    val filter2 = frame.Filter("score < 50")
    filter2.addStepListener(listener)

    cassIn --> filter2 --> filter --> sql
    sql.evaluate

    import frame.ReduceOp._
    val reduce = frame.Reduce() % MIN("score") % MAX("score")
    reduce.addStepListener(listener)

    filter2.out(1) --> reduce
    reduce.evaluate

    shutDown
  }

  sys.exit(0)

  private def setUp() = {
    val session = cluster.connect(keyspace)
    initData(session, 20)
    session.close
  }

  private def shutDown() = {
    val session = cluster.connect(keyspace)
    session.execute(s"drop table $tbl")
    session.close
  }

  private def initData(session: Session, recordCount: Int = 10) = {
    session.execute(s"""create table if not exists $tbl (
      | id uuid, 
      | name text, 
      | score double, 
      | primary key (id))""".stripMargin)
    session.execute(s"truncate $tbl")

    val names = List("john", "jane", "jake", "jack", "jess", "jill", "josh", "jade", "judd")

    val ps = session.prepare(s"insert into $tbl (id, name, score) values (?, ?, ?)")
    (1 to recordCount) foreach { _ =>
      val id = UUID.randomUUID
      val name = names(Random.nextInt(names.size))
      val score = Random.nextInt(1000) / 10.0
      session.execute(ps.bind(id, name, score: java.lang.Double))
    }
  }
}