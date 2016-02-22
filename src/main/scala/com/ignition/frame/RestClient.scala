package com.ignition.frame

import java.net.URL

import scala.xml.{ Elem, Node }
import scala.Option.option2Iterable
import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{ Duration, MILLISECONDS }

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.types.StructType

import com.ignition.types._
import com.ignition.util.ConfigUtils.{ RichConfig, getConfig }

import com.stackmob.newman._
import com.stackmob.newman.dsl.HeaderBuilder
import com.stackmob.newman.response.HttpResponse
import com.typesafe.config.ConfigException

import org.apache.spark.sql.Row
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import com.ignition.util.XmlUtils._
import com.ignition.util.JsonUtils._

/**
 * Encapsulates HTTP request.
 */
case class RequestInfo(url: URL, body: Option[String], headers: Map[String, String])

/**
 * HTTP method.
 */
object HttpMethod extends Enumeration {

  abstract class HttpMethod extends super.Val {
    def invoke(ri: RequestInfo)(implicit client: HttpClient): Future[HttpResponse]
  }
  implicit def valueToMethod(v: Value) = v.asInstanceOf[HttpMethod]

  val GET = new HttpMethod {
    def invoke(ri: RequestInfo)(implicit client: HttpClient): Future[HttpResponse] =
      dsl.GET(ri.url).addHeaders(ri.headers.toList).toRequest.apply
  }

  val POST = new HttpMethod {
    def invoke(ri: RequestInfo)(implicit client: HttpClient): Future[HttpResponse] = {
      val post = dsl.POST(ri.url)
      val builder = ri.body map post.addBody getOrElse (post) addHeaders (ri.headers.toList)
      builder.toRequest.apply
    }
  }

  val PUT = new HttpMethod {
    def invoke(ri: RequestInfo)(implicit client: HttpClient): Future[HttpResponse] = {
      val post = dsl.PUT(ri.url)
      val builder = ri.body map post.addBody getOrElse (post) addHeaders (ri.headers.toList)
      builder.toRequest.apply
    }
  }

  val DELETE = new HttpMethod {
    def invoke(ri: RequestInfo)(implicit client: HttpClient): Future[HttpResponse] =
      dsl.DELETE(ri.url).addHeaders(ri.headers.toList).toRequest.apply
  }

  val HEAD = new HttpMethod {
    def invoke(ri: RequestInfo)(implicit client: HttpClient): Future[HttpResponse] = {
      dsl.HEAD(ri.url).addHeaders(ri.headers.toList).toRequest.apply
    }
  }

  def execute(hb: HeaderBuilder): Future[HttpResponse] = hb.toRequest.apply
}

/**
 * HTTP REST Client, executes one request per row.
 *
 * @author Vlad Orzhekhovskiy
 */
case class RestClient(url: String, method: HttpMethod.HttpMethod = HttpMethod.GET, body: Option[String] = None,
                      headers: Map[String, String] = Map.empty, resultField: Option[String] = Some("result"),
                      statusField: Option[String] = Some("status"), headersField: Option[String] = None)
  extends FrameTransformer {

  import RestClient._

  def method(method: HttpMethod.HttpMethod): RestClient = copy(method = method)
  def body(body: String): RestClient = copy(body = Some(body))
  def headers(headers: Map[String, String]) = copy(headers = headers)
  def header(tuple: (String, String)) = headers(this.headers + tuple)
  def header(name: String, value: String) = headers(this.headers + (name -> value))
  def result(fieldName: String) = copy(resultField = Some(fieldName))
  def noResult() = copy(resultField = None)
  def status(fieldName: String) = copy(statusField = Some(fieldName))
  def noStatus() = copy(statusField = None)
  def responseHeaders(fieldName: String) = copy(headersField = Some(fieldName))

  protected def compute(arg: DataFrame)(implicit runtime: SparkRuntime): DataFrame = {
    val url = this.url
    val method = this.method
    val body = this.body
    val headers = this.headers
    val resultField = this.resultField
    val statusField = this.statusField
    val headersField = this.headersField

    val indexMap = arg.schema.indexMap

    val df = optLimit(arg, runtime.previewMode)

    val rdd = df mapPartitions { rows =>
      implicit val httpClient = new ApacheHttpClient

      val responses = rows map { row =>
        val rowUrl = injectAll(row, indexMap)(url)
        val ri = RequestInfo(new URL(rowUrl), body, headers)
        val frsp = method.invoke(ri) map { rsp =>
          val result = rsp.bodyString
          val headers = compact(render(rsp.headers.map(_.list).getOrElse(Nil).toList))
          val status = rsp.code.code
          val fields = resultField.map(_ => result).toSeq ++ statusField.map(_ => status) ++ headersField.map(_ => headers)
          Row.fromSeq(row.toSeq ++ fields)
        }
        frsp
      }

      Await.result(Future.sequence(responses), RestClient.maxTimeout)
    }

    ctx.createDataFrame(rdd, computeSchema)
  }
  
  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType = computeSchema

  private def computeSchema(implicit runtime: SparkRuntime): StructType = {
    val inSchema = input.schema
    val fields = inSchema ++ resultField.map(string(_)) ++ statusField.map(int(_)) ++ headersField.map(string(_))
    StructType(fields)
  }

  def toXml: Elem =
    <node>
      <url>{ url }</url>
      <method>{ method.toString }</method>
      { body map (b => <body>{ b }</body>) toList }
      {
        if (!headers.isEmpty)
          <headers>
            { headers map (h => <header name={ h._1 }>{ h._2 }</header>) }
          </headers>
      }
      { resultField map (f => <resultField name={ f }/>) toList }
      { statusField map (f => <statusField name={ f }/>) toList }
      { headersField map (f => <headersField name={ f }/>) toList }
    </node>.copy(label = tag)

  def toJson: JValue = {
    val jHeaders = if (headers.isEmpty) None else Some(headers.map(h => ("name" -> h._1) ~ ("value" -> h._2)))
    ("tag" -> tag) ~ ("url" -> url) ~ ("method" -> method.toString) ~
      ("body" -> body) ~ ("headers" -> jHeaders) ~
      ("resultField" -> resultField) ~ ("statusField" -> statusField) ~ ("headersField" -> headersField)
  }
}

/**
 * REST Client companion object.
 */
object RestClient {
  import com.ignition.util.ConfigUtils._

  val tag = "rest-client"

  val maxTimeout = {
    val millis = getConfig("rest").getTimeInterval("max-timeout").getMillis
    Duration.apply(millis, MILLISECONDS)
  }

  def fromXml(xml: Node) = {
    val url = xml \ "url" asString
    val method = HttpMethod.withName(xml \ "method" asString).asInstanceOf[HttpMethod.HttpMethod]
    val body = xml \ "body" getAsString
    val headers = xml \ "headers" \ "header" map { node =>
      val name = node \ "@name" asString
      val value = node.child.head asString

      name -> value
    } toMap
    val resultField = xml \ "resultField" \ "@name" getAsString
    val statusField = xml \ "statusField" \ "@name" getAsString
    val headersField = xml \ "headersField" \ "@name" getAsString

    apply(url, method, body, headers, resultField, statusField, headersField)
  }

  def fromJson(json: JValue) = {
    val url = json \ "url" asString
    val method = HttpMethod.withName(json \ "method" asString).asInstanceOf[HttpMethod.HttpMethod]
    val body = json \ "body" getAsString
    val headers = (json \ "headers" asArray) map { node =>
      val name = node \ "name" asString
      val value = node \ "value" asString

      name -> value
    } toMap
    val resultField = json \ "resultField" getAsString
    val statusField = json \ "statusField" getAsString
    val headersField = json \ "headersField" getAsString

    apply(url, method, body, headers, resultField, statusField, headersField)
  }
}