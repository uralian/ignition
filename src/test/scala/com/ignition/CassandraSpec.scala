package com.ignition

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.blocking

import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.CQLDataSet
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper

import com.datastax.driver.core._
import com.ignition.util.ConfigUtils

/**
 * Companion object used to initialize the cassandra cluster and expose on port
 * depending on the configuration settings.
 */
object CassandraBaseTestHelper {

  private val config = ConfigUtils.getConfig("cassandra.test")

  val embeddedMode = config.getBoolean("embedded")

  val (host, port, thriftPort) = if (embeddedMode) ("localhost", 9142, 9175) else
    (config.getString("external.host"), config.getInt("external.port"), config.getInt("external.thrift-port"))

  Console.println(s"Using ${if (embeddedMode) "Embedded" else "External"} Cassandra at $host:$port")

  val cluster = Cluster.builder()
    .addContactPoints(host)
    .withPort(port)
    .withoutJMXReporting()
    .withoutMetrics()
    .build()

  lazy val session = blocking { cluster.connect() }
}

/**
 * A trait that wraps cassandra-unit package, exposing useful function for embedding
 * Cassandra for unit testing purposes.  cassandra-unit provide similar features as
 * db-unit but for the Cassandra database.  It extends [[com.ignition.BeforeAllAfterAll]]
 * overriding the before and after all functions to control the Cassandra instance,
 * `keyspace` creation, selection, droping, and `dataset` loading.
 *
 * @note keySpace partially defined, must be overridden by implementing class.
 *                Represents the keyspace to use for this test case.
 * @note dataSet partially defined, must be overridden by implementing class.
 *               Represents dataset(s) to be loaded for use in this test case.  Can
 *               consist of CQL table definition(s) or data to be loaded via
 *               {{{
 *                   INSERT INTO myTable ....
 *               }}}
 *
 * @note keyspace will be DROPED at the end of all tests for a given specification.
 *
 * @note if overriding beforeAll or afterAll of this trait ensure that you issue a
 *       {{{ super.beforeAll() }}} as these are controlling the life cycle of the
 *       Cassandra instance.
 *
 * @see BeforeAndAfterAll
 */
trait CassandraSpec extends BeforeAllAfterAll {

  val keySpace: String

  val dataSet: String

  val cluster = CassandraBaseTestHelper.cluster
  implicit lazy val session: Session = CassandraBaseTestHelper.session

  private[this] def createKeySpace(spaceName: String) = blocking {
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $spaceName WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
    session.execute(s"USE $spaceName;")
  }

  private[this] def loadCQL(dataSet: CQLDataSet) = new CQLDataLoader(session).load(dataSet)

  override def beforeAll() {
    if (CassandraBaseTestHelper.embeddedMode) {
      EmbeddedCassandraServerHelper.mkdirs()
      EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml")
    }
    loadCQL(new ClassPathCQLDataSet(dataSet, true, true, keySpace))
  }

  override def afterAll() = blocking {
    session.execute(s"DROP KEYSPACE $keySpace;")
  }
}