package com.ignition.stream

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.xml.{ Elem, Node, PCData }

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic
import org.mvel2.{ MVEL, ParserContext }
import org.mvel2.integration.impl.MapVariableResolverFactory

import com.ignition.frame.DataGrid
import com.ignition.script.ScriptFunctions
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Starting point for MVEL-based implementations of the update state function.
 *
 * @param schema the schema of the resulting state.
 * @param expr MVEL expression representing the state function.
 * @param keyFields fields constituting the key.
 *
 * @author Vlad Orzhekhovskiy
 */
abstract class MvelStateUpdate[S <: AnyRef: ClassTag](schema: StructType, expr: String, keyFields: Iterable[String])
  extends StateUpdate[S](keyFields) {

  import MvelStateUpdate._

  def stateClass: Class[S]

  def ensureSerializable(state: S): S

  @transient private lazy val compiled = MvelStateUpdate.synchronized {
    val pctx = new ParserContext
    pctx.addInput("$" + "input", classOf[java.util.List[java.util.Map[String, Any]]])
    pctx.addInput("$state", stateClass)
    ScriptFunctions.getClass.getDeclaredMethods foreach { method =>
      pctx.addImport(method.getName, method)
    }

    MVEL.compileExpression(expr, pctx)
  }

  def stateFunc(input: Seq[Row], oldState: Option[S]): Option[S] = {
    val mvelInput = input map MvelStateUpdate.row2javaMap asJava
    val mvelState = oldState getOrElse null

    val args = new java.util.HashMap[String, Any]()
    // removes a pesky warning about possible interpolator
    args.put("$" + "input", mvelInput)
    args.put("$" + "state", mvelState)
    val factory = new MapVariableResolverFactory(args)

    val newState = MVEL.executeExpression(compiled, ScriptFunctions, args, stateClass)
    Option(newState) map ensureSerializable
  }

  def toXml: Elem =
    <node>
      <stateClass>{ stateClass.getSimpleName }</stateClass>
      { DataGrid.schemaToXml(schema) }
      <function>{ PCData(expr) }</function>
      <keys>
        { keyFields map (f => <field name={ f }/>) }
      </keys>
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("stateClass" -> stateClass.getSimpleName) ~
    ("schema" -> DataGrid.schemaToJson(schema)) ~ ("function" -> expr) ~ ("keys" -> keyFields)
}

/**
 * MVEL update state companion object.
 */
object MvelStateUpdate {
  val tag = "stream-state-mvel"

  val mapTag = classOf[MapState].getSimpleName
  val listTag = classOf[MapListState].getSimpleName

  def fromXml(xml: Node) = {
    val stateClass = xml \ "stateClass" asString
    val schema = DataGrid.xmlToSchema((xml \ "schema").head)
    val expr = xml \ "function" asString
    val keyFields = (xml \ "keys" \ "field") map (_ \ "@name" asString)
    stateClass match {
      case `mapTag` => MvelMapStateUpdate(schema, expr, keyFields)
      case `listTag` => MvelMapListStateUpdate(schema, expr, keyFields)
    }
  }

  def fromJson(json: JValue) = {
    val stateClass = json \ "stateClass" asString
    val schema = DataGrid.jsonToSchema(json \ "schema")
    val expr = json \ "function" asString
    val keyFields = (json \ "keys" asArray) map (_ asString)
    stateClass match {
      case `mapTag` => MvelMapStateUpdate(schema, expr, keyFields)
      case `listTag` => MvelMapListStateUpdate(schema, expr, keyFields)
    }
  }

  def row2javaMap(row: Row): java.util.Map[String, Any] = (row.toSeq zip row.schema map {
    case (value, field) => field.name -> value
  } toMap) asJava

  def javaMap2row(schema: StructType)(data: java.util.Map[String, Any]): Row = {
    val scalaMap = data.asScala
    val values = schema.map(f => data.asScala.get(f.name).orNull)
    new GenericRowWithSchema(values.toArray, schema)
  }
}

/**
 * Models state as a java Map[String, Any]. The map should contain only simple values, not collections.
 */
case class MvelMapStateUpdate(schema: StructType, expr: String, keyFields: Iterable[String])
  extends MvelStateUpdate[MapState](schema, expr, keyFields) {

  import MvelStateUpdate._

  def code(str: String) = copy(expr = str)
  def keys(k: String*) = copy(keyFields = k)

  val stateClass = classOf[MapState]

  def ensureSerializable(state: MapState): MapState = state match {
    case _: Serializable => state
    case _ => new java.util.HashMap(state)
  }

  def mapFunc(state: MapState): Iterable[Row] = Seq(javaMap2row(schema)(state))
}

object MvelMapStateUpdate {
  def apply(schema: StructType, expr: String, keyFields: String*): MvelMapStateUpdate =
    apply(schema, expr, keyFields)
  def apply(schema: StructType): MvelMapStateUpdate = apply(schema, null, Nil)
}

/**
 * Models state as a java Iterable[Map[String, Any]].
 * Each map should contain only simple values, not collections.
 */
case class MvelMapListStateUpdate(schema: StructType, expr: String, keyFields: Iterable[String])
  extends MvelStateUpdate[MapListState](schema, expr, keyFields) {

  import MvelStateUpdate._

  def code(str: String) = copy(expr = str)
  def keys(k: String*) = copy(keyFields = k)

  val stateClass = classOf[MapListState]

  def ensureSerializable(state: MapListState): MapListState = state match {
    case _: Serializable => state
    case _ =>
      val list = new java.util.ArrayList[java.util.Map[String, Any]]
      state.asScala foreach list.add
      list
  }

  def mapFunc(state: MapListState): Iterable[Row] = state.asScala map javaMap2row(schema)
}

object MvelMapListStateUpdate {
  def apply(schema: StructType, expr: String, keyFields: String*): MvelMapListStateUpdate =
    apply(schema, expr, keyFields)
  def apply(schema: StructType): MvelMapListStateUpdate = apply(schema, null, Nil)
}
