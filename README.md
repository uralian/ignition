# Ignition

![alt tag](https://travis-ci.org/uralian/ignition.svg?branch=master)
![Coverage Status](https://coveralls.io/repos/uralian/ignition/badge.svg)

Ignition is a tool for creating reusable workflows for Apache Spark.
Inspired by [Pentaho Kettle](http://community.pentaho.com/projects/data-integration/),
it is an attempt to provide data analysts with means to create Spark-based analytical
workflows without the need to learn Scala, Java, or Python.

A workflow is a directed graph where data travels the edges connecting various input
and transformation nodes called `steps`. Each step can have an arbitrary number of input
and output ports that are used to connect it to other steps. Data traveling between steps
is represented by Spark DataFrames, i.e. represents a regular grid with a fixed
number of columns, each having a name and a type (similar to a relational DB table).

## Features
- The followig data types are supported:
 - `binary` (array of bytes)
 - `boolean`
 - `string`
 - `byte`
 - `short`
 - `integer`
 - `long`
 - `float`
 - `double`
 - `decimal`
 - `date`
 - `timestamp`
- The following pre-defined step types are available:
 - Producer (0 inputs, 1 output)
 - Transformer (1 input, 1 output)
 - Splitter (1 input, >1 outputs)
 - Merger (>1 inputs, 1 output)
 - Module (N inputs, M outputs) 
 - SubFlow (N input, M outputs &ndash; contains a a subgraph of steps)
- Supports named Spark broadcasts and accumulators
- Uses native Spark map-reduce engine for data processing
- Supports SQL queries through SparkSQL
- Contains more than 30 steps out of the box
- Provides an intuitive DSL for building and connecting the steps
- Supports JSON and XML serialization of workflows (under development)
 
## Download
This project has been published to the Maven Central Repository.
For SBT to download the connector binaries, sources and javadoc, put this in your project 
SBT config:
                                                                                                                           
    libraryDependencies += "com.uralian" %% "ignition" % "0.2.0"
    
Ignition allows to run spark in embedded mode by using the special `local[*]`
Spark master URL. For deployment on a spark cluster, refer to Apache Spark
application 
[deployment guide](https://spark.apache.org/docs/latest/cluster-overview.html).

## 5-minute start guide

```scala
import com.ignition.SparkPlug
import com.ignition.frame._
import com.ignition.types._

import BasicAggregator._

val flow = DataFlow {
 
  // create an input data grid with 4 columns of the specified types and fill it with test data
  // in real life, one will probably use CassandraInput or TextFileInput etc.
  val grid1 = DataGrid(string("id") ~ string("name") ~ int("weight") ~ date("dob")) rows (
    ("j1", "john", 155, date(1980, 5, 2)),
    ("j2", "jane", 190, date(1982, 4, 25)),
    ("j3", "jake", 160, date(1974, 11, 3)),
    ("j4", "josh", 120, date(1995, 1, 10))
  )

  // create another input data grid with 1 column and two rows.
  // when a row contains only one element, you can skip ()
  val grid2 = DataGrid(string("name")) rows ("jane", "josh")

  // our first pipeline will use SQL join to merge the input grids by 'name' field.
  // SQLQuery step is a merger, i.e. it can have any number of inputs and 1 output
  // in SQL, you can refer to the inputs using names input0, input1, etc.
  val queryA = SQLQuery("""
    SELECT SUM(weight) AS total, AVG(weight) AS mean, MIN(weight) AS low
    FROM input0 JOIN input1 ON input0.name = input1.name
    WHERE input0.name LIKE 'j%'""")

  // SelectValues step is a transformer, which allows renaming the data columns,
  // change data types, retain or delete columns
  val selectA = SelectValues() rename ("mean" -> "average") retype ("average" -> "int")

  // DebugOutput() simply prints the data and passes it through to the next node, if connected
  val debugA = DebugOutput()

  // this notation means, that the outputs of steps grid1 and grid2 need to be
  // connected to the inputs 0 and 1 of step queryA, the output of queryA &ndash;
  // to the input of step selectA, and finally the output of selectA &ndash;
  // to the input of debugA 
  (grid1, grid2) --> queryA --> selectA --> debugA

  // the second pipeline also uses SQLQuery to process the data from one input
  val queryB = SQLQuery("SELECT SUBSTR(name, 1, 2) AS name, weight FROM input0")

  // BasicStats() provides aggregation functions and grouping
  val statsB = BasicStats() groupBy ("name") aggr ("weight", AVG, MAX, COUNT_DISTINCT)

  // another debug output
  val debugB = DebugOutput()

  // a simple pipeline of steps
  grid1 --> queryB --> statsB --> debugB

  // Ignition uses a lazy evaluation model, i.e. only those steps that contribute
  // to the final result will be evaluated. The last line of the DataFlow definition
  // needs to contain a list of terminal nodes that need to be evaluated.
  (debugA, debugB)
}

// SparkPlug helper object connects to the Spark runtime and runs the data flow.
SparkPlug.runDataFlow(flow)
```

The code above should produce these results: 

    +----------+----------+----------+
    |     total|   average|       low|
    +----------+----------+----------+
    |       310|       155|       120|
    +----------+----------+----------+
    +----------+----------+----------+---------------+
    |      name|weight_avg|weight_max|weight_cnt_dist|
    +----------+----------+----------+---------------+
    |        ja|175.000000|       190|              2|
    |        jo|137.500000|       155|              2|
    +----------+----------+----------+---------------+
