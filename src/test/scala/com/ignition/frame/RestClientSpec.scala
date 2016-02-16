package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types.{ RichStructType, fieldToRichStruct, int, string }

@RunWith(classOf[JUnitRunner])
class RestClientSpec extends FrameFlowSpecification {
  sequential
  
  import HttpMethod._

  System.setProperty("apikey", "2a87e6d26f5fd1d507fea7dfbbcf35d8")

  val schema = string("city") ~ string("country")
  val grid = DataGrid(schema).addRow("london", "uk").addRow("atlanta", "us")

  //TODO: real calls to be replaced with mocks

  "RestClient" should {
//    "return valid result and status" in {
//      System.setProperty("weather_url", "http://api.openweathermap.org/data/2.5/weather")
//      rt.vars("query") = "q"
//      val url = "e{weather_url}?v{query}=$" + "{city},$" + "{country}&appid=e{apikey}"
//      val client = RestClient(url) result "result" status "status"
//      grid --> client
//
//      assertSchema(schema ~ string("result") ~ int("status"), client, 0)
//      client.output.collect.forall(!_.getString(2).isEmpty)
//      client.output.collect.forall(_.getInt(3) == 200)
//    }
//    "return valid result and headers" in {
//      val url = "http://api.openweathermap.org/data/2.5/weather?q=$" + "{city},$" + "{country}&APPID=e{apikey}"
//      val client = RestClient(url, GET) result "result" noStatus () responseHeaders "headers"
//      grid --> client
//
//      assertSchema(schema ~ string("result") ~ string("headers"), client, 0)
//      client.output.collect.forall(!_.getString(3).isEmpty)
//    }
//    "return failure code for missing key" in {
//      val url = "http://api.openweathermap.org/data/2.5/unknown"
//      val client = RestClient(url, GET, None, Map.empty, None, Some("status"), None)
//      grid --> client
//      
//      assertSchema(schema ~ int("status"), client, 0)
//      client.output.collect.forall(_.getInt(2) == 401)
//    }
//    "return failure code for bad path" in {
//      val url = "http://api.openweathermap.org/data/2.5/unknown?APPID=e{apikey}"
//      val client = RestClient(url, GET, None, Map.empty, None, Some("status"), None)
//      grid --> client
//      
//      assertSchema(schema ~ int("status"), client, 0)
//      client.output.collect.forall(_.getInt(2) != 200)
//    }
//    "fail for unknown host" in {
//      val url = "http://unknown_host_for_ignition_testing.org"
//      val client = RestClient(url, GET, None, Map.empty, None, Some("status"), None)
//      grid --> client
//
//      assertSchema(schema ~ int("status"), client, 0)
//      client.output.collect must throwA[Exception]
//    }
    "save to/load from xml" in {
      val c1 = RestClient("yahoo.com") method GET result "result" status "status" body "msg"
      c1.toXml must ==/(
        <rest-client>
          <url>yahoo.com</url>
          <method>GET</method>
          <body>msg</body>
          <resultField name="result"/>
          <statusField name="status"/>
        </rest-client>)
      RestClient.fromXml(c1.toXml) === c1

      val c2 = RestClient("yahoo.com") method GET noResult () noStatus () header ("a" -> "b")
      c2.toXml must ==/(
        <rest-client>
          <url>yahoo.com</url>
          <method>GET</method>
          <headers>
            <header name="a">b</header>
          </headers>
        </rest-client>)
      RestClient.fromXml(c2.toXml) === c2
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val c1 = RestClient("yahoo.com") method GET result "result" status "status"
      c1.toJson === ("tag" -> "rest-client") ~ ("url" -> "yahoo.com") ~ ("method" -> "GET") ~
        ("body" -> jNone) ~ ("headers" -> jNone) ~ ("headersField" -> jNone) ~
        ("resultField" -> "result") ~ ("statusField" -> "status")
      RestClient.fromJson(c1.toJson) === c1

      val c2 = RestClient("yahoo.com") method GET body "msg" noResult () noStatus () header ("a" -> "b")
      c2.toJson === ("tag" -> "rest-client") ~ ("url" -> "yahoo.com") ~ ("method" -> "GET") ~
        ("body" -> "msg") ~ ("headers" -> List(("name" -> "a") ~ ("value" -> "b"))) ~
        ("resultField" -> jNone) ~ ("statusField" -> jNone) ~ ("headersField" -> jNone)
      RestClient.fromJson(c2.toJson) === c2
    }
    "be unserializable" in assertUnserializable(RestClient("http://localhost", GET, None, Map.empty))
  }
}