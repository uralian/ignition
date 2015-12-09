package com.ignition.samples

import org.apache.spark.sql.types.DoubleType
import org.dsa.iot.spark.RichInt

import com.ignition.frame.SQLQuery
import com.ignition.frame.mllib.Correlation
import com.ignition.stream
import com.ignition.stream.{ DSAStreamInput, DSAStreamOutput, StreamFlow, Window, foreach }

object DSALinkFlow extends App {

  val flow = StreamFlow {

    val dsaIn = DSAStreamInput() %
      ("/downstream/System/Memory_Usage" -> DoubleType) %
      ("/downstream/System/CPU_Usage" -> DoubleType)

    val sql = foreach { SQLQuery("select AVG(CPU_Usage) as cpu, AVG(Memory_Usage) as mem from input0") }

    val win = Window(30 seconds)

    val corr = foreach { Correlation() % "cpu" % "mem" }

    val dsaOut = DSAStreamOutput() % ("corr_cpu_mem" -> "/output/mem_cpu_correlation")

    dsaIn --> sql --> win --> corr --> dsaOut

    (dsaOut)
  }

  stream.Main.startStreamFlow(flow)
}