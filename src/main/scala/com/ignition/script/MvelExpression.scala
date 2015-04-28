package com.ignition.script

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.reflect.runtime.universe.runtimeMirror

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import org.mvel2.{ MVEL, ParserContext }

/**
 * Mvel expression processor. Uses strict mode for the best performance.
 *
 * @author Vlad Orzhekhovskiy
 */
case class MvelExpression(val expression: String) extends RowExpression[DataType] {
  @transient private var compiled: java.io.Serializable = null

  def targetType = None

  def evaluate(schema: StructType)(row: Row) = {
    if (compiled == null) MvelExpression.synchronized {
      val parserContext = createParserContext(schema)
      compiled = MVEL.compileExpression(expression, parserContext)
    }
    val args = row2javaMap(schema)(row)
    MVEL.executeExpression(compiled, ScriptFunctions, args)
  }

  private def createParserContext(schema: StructType) = {
    val pctx = new ParserContext
    pctx.setStrictTypeEnforcement(true)
    val mirror = runtimeMirror(getClass.getClassLoader)
    val inputs: Seq[(String, Class[_])] = schema map {
      case StructField(name, BinaryType, _, _) => name -> classOf[Array[Byte]]
      case StructField(name, BooleanType, _, _) => name -> classOf[Boolean]
      case StructField(name, StringType, _, _) => name -> classOf[String]
      case StructField(name, ByteType, _, _) => name -> classOf[Byte]
      case StructField(name, ShortType, _, _) => name -> classOf[Short]
      case StructField(name, IntegerType, _, _) => name -> classOf[Integer]
      case StructField(name, LongType, _, _) => name -> classOf[Long]
      case StructField(name, FloatType, _, _) => name -> classOf[Float]
      case StructField(name, DoubleType, _, _) => name -> classOf[Double]
      case StructField(name, _: DecimalType, _, _) => name -> classOf[java.math.BigDecimal]
      case StructField(name, DateType, _, _) => name -> classOf[java.sql.Date]
      case StructField(name, TimestampType, _, _) => name -> classOf[java.sql.Timestamp]
    }
    pctx.addInputs(inputs.toMap.asJava)
    ScriptFunctions.getClass.getDeclaredMethods foreach { method =>
      pctx.addImport(method.getName, method)
    }
    pctx
  }
}