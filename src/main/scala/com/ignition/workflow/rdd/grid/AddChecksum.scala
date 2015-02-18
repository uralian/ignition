package com.ignition.workflow.rdd.grid

import java.security.MessageDigest

import scala.Array.canBuildFrom
import scala.collection.TraversableOnce.MonadOps
import scala.xml.{ Elem, Node }

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.ignition.data.{ Binary, DataRow, DefaultDataRow, DefaultRowMetaData, RowMetaData }
import com.ignition.workflow.Step1

/**
 * Checksum algorithm.
 *
 * @author Vlad Orzhekhovskiy
 */
sealed abstract class DigestAlgorithm(val code: String) extends Serializable {
  def digest(data: Array[Byte]): Array[Byte] = MessageDigest.getInstance(code).digest(data)
}
object DigestAlgorithm {
  case object MD5 extends DigestAlgorithm("MD5")
  case object SHA1 extends DigestAlgorithm("SHA-1")
  case object SHA256 extends DigestAlgorithm("SHA-256")

  def withCode(code: String): DigestAlgorithm = code match {
    case MD5.code => MD5
    case SHA1.code => SHA1
    case SHA256.code => SHA256
  }
}

/**
 * Add Checksum.
 */
case class AddChecksum(name: String, algorithm: DigestAlgorithm,
  fields: TraversableOnce[String], outputAsString: Boolean = true)
  extends GridStep1 {

  protected def computeRDD(rdd: RDD[DataRow]): RDD[DataRow] = rdd map { row =>
    val binary = fields.map(name => algorithm.digest(row.getBinary(name))).foldLeft(Array[Byte]())(_ ++ _)

    val chksum: Any = if (outputAsString) binary.map("%02X" format _).mkString else binary

    val newColumnNames = row.columnNames :+ name
    val newData = row.rawData :+ chksum
    DefaultDataRow(newColumnNames, newData)
  }

  def outMetaData: Option[RowMetaData] = inMetaData map { mdIn =>
    val rmd = DefaultRowMetaData(mdIn.columns)
    if (outputAsString) rmd.add[String](name) else rmd.add[Binary](name)
  }

  def toXml: Elem =
    <add-checksum name={ name } algorithm={ algorithm.code } output={ if (outputAsString) "string" else "binary" }>
      {
        fields map (field => <col name={ field }/>)
      }
    </add-checksum>
}

/**
 * Add Checksum companion object.
 */
object AddChecksum extends XmlFactory[AddChecksum] {
  def fromXml(xml: Node) = {
    val name = (xml \ "@name").text
    val algorithm = DigestAlgorithm.withCode((xml \ "@algorithm").text)
    val fields = (xml \ "col" \\ "@name") map (_.text)
    val textOutput = if ((xml \ "@output").text == "binary") false else true
    AddChecksum(name, algorithm, fields, textOutput)
  }
}