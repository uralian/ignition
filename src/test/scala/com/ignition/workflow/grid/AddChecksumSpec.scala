package com.ignition.workflow.grid

import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.ignition.SparkTestHelper
import com.ignition.data._
import com.ignition.data.DataType.{ BinaryDataType, BooleanDataType, IntDataType, StringDataType }
import com.ignition.workflow.rdd.grid.{ AddChecksum, DigestAlgorithm }
import com.ignition.workflow.rdd.grid.input.DataGridInput

@RunWith(classOf[JUnitRunner])
class AddChecksumSpec extends Specification with XmlMatchers with SparkTestHelper {

  val meta = string("a") ~ int("b") ~ boolean("c") 
  val row = meta.row("25.2", 10, true)

  "AddChecksum" should {
    "save to xml" in {
      val step = AddChecksum("chk", DigestAlgorithm.SHA1, List("a", "b", "c"), false)
      <add-checksum name="chk" algorithm="SHA-1" output="binary">
        <col name="a"/><col name="b"/><col name="c"/>
      </add-checksum> must ==/(step.toXml)
    }
    "load from xml" in {
      val step = AddChecksum.fromXml(
        <add-checksum name="chk" algorithm="MD5" output="string">
          <col name="a"/><col name="b"/>
        </add-checksum>)
      step === AddChecksum("chk", DigestAlgorithm.MD5, List("a", "b"), true)
    }
    "add checksum column" in {
      val grid = DataGridInput(meta, Seq(row))
      val step = AddChecksum("chk", DigestAlgorithm.SHA1, List("c"), false)
      grid.connectTo(step)
      val expectedMeta = meta.add[Binary]("chk")
      step.outMetaData === Some(expectedMeta)
      step.output.collect.iterator.next.columnNames === Vector("a", "b", "c", "chk")
    }
  }
}