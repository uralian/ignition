package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types.{ fieldToStructType, string }

@RunWith(classOf[JUnitRunner])
class CacheSpec extends FrameFlowSpecification {

  "Cache" should {
    "leave data unchanged" in {
      val grid = DataGrid(string("name")) rows (Tuple1("john"), Tuple1("jim"), Tuple1("jake"))
      val cache = Cache()
      grid --> cache
      assertOutput(cache, 0, Tuple1("john"), Tuple1("jim"), Tuple1("jake"))
      assertSchema(string("name"), cache, 0)
    }
    "save to/load from xml" in {
      val cache = Cache()
      cache.toXml must ==/(<cache/>)
      Cache.fromXml(cache.toXml) === cache
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._
      import org.json4s.JValue

      val cache = Cache()
      cache.toJson === (("tag" -> "cache"): JValue)
      Cache.fromJson(cache.toJson) === cache
    }
  }
}