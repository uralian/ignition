package com.ignition.flow

import java.net.URL
import scala.Option.option2Iterable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.{ Duration, MILLISECONDS }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.types.StructType
import com.ignition.types.{ int, string }
import com.ignition.util.ConfigUtils.{ RichConfig, getConfig }
import com.stackmob.newman._
import com.stackmob.newman.dsl.HeaderBuilder
import com.stackmob.newman.response.HttpResponse
import com.typesafe.config.ConfigException
import net.liftweb.json.{ compact, render }
import net.liftweb.json.JsonDSL.{ int2jvalue, option2jvalue, pair2jvalue, seq2jvalue, string2jvalue }
import org.apache.spark.sql.Row

/**
 * Encapsulates HTTP request.
 */
case class RequestInfo(url: URL, body: Option[String], headers: Map[String, String])

/**
 * HTTP method.
 */
object HttpMethod extends Enumeration {

  trait HttpMethod extends super.Val {
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
case class RestClient(url: String, method: HttpMethod.HttpMethod, body: Option[String] = None,
  headers: Map[String, String] = Map.empty, resultField: Option[String] = Some("result"),
  statusField: Option[String] = Some("status"), headersField: Option[String] = None) extends Transformer {

  def body(body: String): RestClient = copy(body = Some(body))
  def headers(headers: Map[String, String]) = copy(headers = headers)
  def header(name: String, value: String) = headers(this.headers + (name -> value))
  def result(fieldName: String) = copy(resultField = Some(fieldName))
  def noResult = copy(resultField = None)
  def status(fieldName: String) = copy(statusField = Some(fieldName))
  def noStatus = copy(statusField = None)
  def responseHeaders(fieldName: String) = copy(headersField = Some(fieldName))

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit ctx: SQLContext): DataFrame = {
    val url = this.url
    val method = this.method
    val body = this.body
    val headers = this.headers
    val resultField = this.resultField
    val statusField = this.statusField
    val headersField = this.headersField

    val indices = arg.schema.fieldNames.zipWithIndex.toMap

    val df = limit map arg.limit getOrElse arg

    val rdd = df mapPartitions { rows =>
      implicit val httpClient = new ApacheHttpClient

      val responses = rows map { row =>
        val rowUrl = indices.foldLeft(url) {
          case (result, (name, index)) => result.replace("${" + name + "}", row.getString(index))
        }
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

    ctx.createDataFrame(rdd, outSchema)
  }

  protected def computeSchema(inSchema: StructType)(implicit ctx: SQLContext): StructType = {
    val fields = inSchema ++ resultField.map(string(_)) ++ statusField.map(int(_)) ++ headersField.map(string(_))
    StructType(fields)
  }

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * REST Client companion object.
 */
object RestClient {
  import com.ignition.util.ConfigUtils._

  val maxTimeout = {
    val millis = getConfig("rest").getTimeInterval("max-timeout").getMillis
    Duration.apply(millis, MILLISECONDS)
  }
}