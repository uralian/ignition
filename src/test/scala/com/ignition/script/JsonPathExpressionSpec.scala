package com.ignition.script

import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.ignition.types.{ fieldToStruct, string }

@RunWith(classOf[JUnitRunner])
class JsonPathExpressionSpec extends Specification {

  val payload = """
{ "store": {
    "book": [
      { "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      { "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      },
      { "category": "fiction",
        "author": "Herman Melville",
        "title": "Moby Dick",
        "isbn": "0-553-21311-3",
        "price": 8.99
      },
      { "category": "fiction",
        "author": "J. R. R. Tolkien",
        "title": "The Lord of the Rings",
        "isbn": "0-395-19395-8",
        "price": 22.99
      }
    ],
    "bicycle": {
      "color": "red",
      "price": 19.95
    }
  }
}"""

  val schema = string("payload").schema
  val row = Row(payload)

  "JSON path expressions" should {
    "find child elements" in {
      val proc = "$.store.book[1].title".json("payload")
      proc.evaluate(schema)(row) === "Sword of Honour"
    }
    "find filtered elements" in {
      val proc = "$.store.book[?(@.title=='Moby Dick')].author".json("payload")
      proc.evaluate(schema)(row) === "Herman Melville"
    }
  }
}