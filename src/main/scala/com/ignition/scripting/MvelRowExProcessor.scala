package com.ignition.scripting

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.reflect.runtime.universe.runtimeMirror
import scala.xml.{ Elem, Node }

import org.mvel2.{ MVEL, ParserContext }

import com.ignition.data.{ DataRow, DataType, RowMetaData }

/**
 * Mvel expression processor. If meta is passed, uses strict mode to improve performance.
 *
 * @author Vlad Orzhekhovskiy
 */
case class MvelRowExProcessor[T](val expression: String, var meta: Option[RowMetaData] = None)(implicit val targetType: DataType[T]) extends RowExProcessor[T] {

  @transient private lazy val compiled = meta map { md =>
    val parserContext = createParserContext(md)
    MVEL.compileExpression(expression, parserContext)
  } getOrElse MVEL.compileExpression(expression)

  /**
   * If the metadata has not been set at the construction time, it copies it from the first call.
   * Since the expression is compiled lazily, it gives it a chance to wait until the meta
   * data is actually available.
   */
  def evaluate(meta: Option[RowMetaData])(row: DataRow): T = {
    if (!this.meta.isDefined) this.meta = meta

    val args = row: java.util.Map[String, Any]
    val result = MVEL.executeExpression(compiled, ScriptFunctions, args)
    targetType.convert(result)
  }

  def toXml: Elem = <mvel type={ targetType.code }>{ expression }</mvel>

  private def createParserContext(md: RowMetaData) = MvelRowExProcessor.synchronized {
    val pctx = new ParserContext
    pctx.setStrictTypeEnforcement(true)
    val mirror = runtimeMirror(getClass.getClassLoader)
    val inputs: Map[String, Class[_]] = md.columns.map { ci =>
      val tpe = ci.dataType.targetTypeTag.tpe
      (ci.name, mirror.runtimeClass(tpe))
    } toMap;
    pctx.addInputs(inputs.asJava)
    ScriptFunctions.getClass.getDeclaredMethods foreach { method =>
      pctx.addImport(method.getName, method)
    }
    pctx
  }
}

/**
 * MVEL processor companion object.
 */
object MvelRowExProcessor {
  def fromXml(xml: Node) = {
    val dataType = DataType.withCode((xml \ "@type").text)
    val expression = xml.text
    new MvelRowExProcessor(expression)(dataType)
  }
}