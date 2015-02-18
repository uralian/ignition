package com.ignition.data

import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.eaio.uuid.UUID
import com.ignition.data.DataType.BooleanDataType

@RunWith(classOf[JUnitRunner])
class RowMetaDataSpec extends Specification with XmlMatchers {

  val log = LoggerFactory.getLogger(getClass)

  "RowMetaData" should {
    "accept column collection" in {
      val meta = uuid("id") ~ string("name") ~ decimal("value") ~ datetime("date")
      meta.columnCount == 4
      meta.columnIndex("id") === 0
      meta.columnIndex("name") === 1
      meta.columnIndex("value") === 2
      meta.columnIndex("date") === 3
      meta.columns(0) === ColumnInfo[UUID]("id")
      meta.columns(1) === ColumnInfo[String]("name")
      meta.columns(2) === ColumnInfo[Decimal]("value")
      meta.columns(3) === ColumnInfo[DateTime]("date")
    }
    "save to xml" in {
      val meta = DefaultRowMetaData(
        ColumnInfo[UUID]("id"),
        ColumnInfo[String]("name"),
        ColumnInfo[Decimal]("value"),
        ColumnInfo[DateTime]("date"),
        ColumnInfo[Int]("index"),
        ColumnInfo[Double]("ratio"),
        ColumnInfo[Binary]("data"),
        ColumnInfo[Boolean]("flag"))
      val xml = meta.toXml
      <meta>
        <col name="id" type="uuid"/>
        <col name="name" type="string"/>
        <col name="value" type="decimal"/>
        <col name="date" type="datetime"/>
        <col name="index" type="int"/>
        <col name="ratio" type="double"/>
        <col name="data" type="binary"/>
        <col name="flag" type="boolean"/>
      </meta> must ==/(xml)
    }
    "load from xml" in {
      val xml =
        <meta>
          <col name="id" type="uuid"/>
          <col name="name" type="string"/>
          <col name="value" type="decimal"/>
          <col name="date" type="datetime"/>
          <col name="index" type="int"/>
          <col name="ratio" type="double"/>
          <col name="data" type="binary"/>
          <col name="flag" type="boolean"/>
        </meta>
      val meta = DefaultRowMetaData.fromXml(xml)
      meta.columns(0).dataType === implicitly[DataType[UUID]]
      meta.columns(0).name === "id"
      meta.columns(1).dataType === implicitly[DataType[String]]
      meta.columns(1).name === "name"
      meta.columns(2).dataType === implicitly[DataType[Decimal]]
      meta.columns(2).name === "value"
      meta.columns(3).dataType === implicitly[DataType[DateTime]]
      meta.columns(3).name === "date"
      meta.columns(4).dataType === implicitly[DataType[Int]]
      meta.columns(4).name === "index"
      meta.columns(5).dataType === implicitly[DataType[Double]]
      meta.columns(5).name === "ratio"
      meta.columns(6).dataType === implicitly[DataType[Binary]]
      meta.columns(6).name === "data"
      meta.columns(7).dataType === implicitly[DataType[Boolean]]
      meta.columns(7).name === "flag"
    }
  }
}