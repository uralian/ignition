package com.ignition.frame.mllib

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.GradientDescent
import org.apache.spark.mllib.regression._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.{ doubleRDDToDoubleRDDFunctions, rddToPairRDDFunctions }
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime
import com.ignition.frame.FrameTransformer
import com.ignition.types.double
import com.ignition.util.ConfigUtils.getConfig

import scala.xml._
import org.json4s._
import org.json4s.JsonDSL._
import com.ignition.util.JsonUtils._
import com.ignition.util.XmlUtils._

/**
 * Regression methods.
 */
object RegressionMethod extends Enumeration {
  abstract class RegressionMethod[T <: GeneralizedLinearModel] extends super.Val {
    def algorithm: GeneralizedLinearAlgorithm[T]
  }
  implicit def valueToMethod(v: Value) =
    v.asInstanceOf[RegressionMethod[_ <: GeneralizedLinearModel]]

  val LINEAR = new RegressionMethod[LinearRegressionModel] {
    def algorithm = new LinearRegressionWithSGD
  }

  val RIDGE = new RegressionMethod[RidgeRegressionModel] {
    def algorithm = new RidgeRegressionWithSGD
  }

  val LASSO = new RegressionMethod[LassoModel] {
    def algorithm = new LassoWithSGD
  }
}
import RegressionMethod._

/**
 * Regression configuration.
 */
case class RegressionConfig(
  regressionMethod: RegressionMethod[_ <: GeneralizedLinearModel] = LINEAR,
  iterationCount: Int = 100, stepSize: Double = 1.0, allowIntercept: Boolean = false) {

  def method(method: RegressionMethod[_ <: GeneralizedLinearModel]) =
    copy(regressionMethod = method)
  def iterations(num: Int) = copy(iterationCount = num)
  def step(num: Double) = copy(stepSize = num)
  def intercept(flag: Boolean) = copy(allowIntercept = flag)

  val algorithm = {
    val algorithm = regressionMethod.algorithm
    algorithm.setIntercept(allowIntercept)
    algorithm.optimizer.asInstanceOf[GradientDescent]
      .setNumIterations(iterationCount)
      .setStepSize(stepSize)
    algorithm
  }

  def toXml: Elem =
    <config>
      <method>{ regressionMethod.toString }</method>
      <iterations>{ iterationCount }</iterations>
      <step>{ stepSize }</step>
      <allowIntercept>{ allowIntercept }</allowIntercept>
    </config>

  def toJson: JValue = ("method" -> regressionMethod.toString) ~
    ("iterations" -> iterationCount) ~ ("step" -> stepSize) ~ ("allowIntercept" -> allowIntercept)
}

/**
 * Regression Config companion object.
 */
object RegressionConfig {

  def fromXml(xml: Node) = {
    val method = RegressionMethod.withName(xml \ "method" asString)
    val iterationCount = xml \ "iterations" asInt
    val stepSize = xml \ "step" asDouble
    val allowIntercept = xml \ "allowIntercept" asBoolean

    RegressionConfig(method, iterationCount, stepSize, allowIntercept)
  }

  def fromJson(json: JValue) = {
    val method = RegressionMethod.withName(json \ "method" asString)
    val iterationCount = json \ "iterations" asInt
    val stepSize = json \ "step" asDouble
    val allowIntercept = json \ "allowIntercept" asBoolean

    RegressionConfig(method, iterationCount, stepSize, allowIntercept)
  }
}

/**
 * Computes the regression using MLLib library.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Regression(labelField: String, dataFields: Iterable[String],
                      groupFields: Iterable[String] = Nil, config: RegressionConfig = RegressionConfig())
  extends FrameTransformer with MLFunctions {

  import Regression._

  def label(field: String) = copy(labelField = field)
  def features(fields: String*) = copy(dataFields = fields)
  def groupBy(fields: String*) = copy(groupFields = fields)

  def method(method: RegressionMethod[_ <: GeneralizedLinearModel]) = copy(config = config.method(method))
  def iterations(num: Int) = copy(config = config.iterations(num))
  def step(num: Double) = copy(config = config.step(num))
  def intercept(flag: Boolean) = copy(config = config.intercept(flag))

  protected def compute(arg: DataFrame, preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    val df = optLimit(arg, preview)

    val rdd = toLabeledPoints(df, labelField, dataFields, groupFields)
    rdd.persist
    val rddSize = rdd.count

    val keys = rdd.keys.distinct.collect

    val solver = if (rddSize < rddSizeThreshold || rddSize / keys.size < rowsPerKeyThreshold)
      solveWithCommons _
    else
      solveWithMLLib(config) _

    val rows = keys map { key =>
      val slice = rdd filter (_._1 == key) values

      val (weights, intercept, r2) = solver(slice)

      Row.fromSeq(key.toSeq ++ weights :+ intercept :+ r2)
    }

    val targetRDD = ctx.sparkContext.parallelize(rows)
    val fieldSeq = dataFields.toSeq
    val targetFields = (groupFields map df.schema.apply toSeq) ++
      dataFields.map(name => double(name + "_weight")) :+ double("intercept") :+ double("r2")
    val schema = StructType(targetFields)

    ctx.createDataFrame(targetRDD, schema)
  }

  def toXml: Elem =
    <node>
      <label>{ labelField }</label>
      <features>
        {
          dataFields map { name => <field name={ name }/> }
        }
      </features>
      {
        if (!groupFields.isEmpty)
          <group-by>
            { groupFields map (f => <field name={ f }/>) }
          </group-by>
      }
      { config.toXml }
    </node>.copy(label = tag)

  def toJson: org.json4s.JValue = {
    val groupBy = if (groupFields.isEmpty) None else Some(groupFields)
    val features = dataFields map (_.toString)

    ("tag" -> tag) ~ ("label" -> labelField) ~ ("features" -> features) ~ ("groupBy" -> groupBy) ~
      ("config" -> config.toJson)
  }
}

/**
 * Regression companion object.
 */
object Regression {
  val tag = "regression"

  def apply(labelField: String, dataFields: String*): Regression = apply(labelField, dataFields)

  def fromXml(xml: Node) = {
    val labelField = xml \ "label" asString
    val dataFields = (xml \ "features" \ "field") map { _ \ "@name" asString }
    val groupFields = (xml \ "group-by" \ "field") map (_ \ "@name" asString)
    val config = RegressionConfig.fromXml(xml \ "config" head)
    Regression(labelField, dataFields, groupFields, config)
  }

  def fromJson(json: JValue) = {
    val labelField = json \ "label" asString
    val dataFields = (json \ "features" asArray) map (_ asString)
    val groupFields = (json \ "groupBy" asArray) map (_ asString)
    val config = RegressionConfig.fromJson(json \ "config")
    Regression(labelField, dataFields, groupFields, config)
  }

  private val cfg = getConfig("mllib.regression")
  val rddSizeThreshold = cfg.getInt("rdd-size-threshold")
  val rowsPerKeyThreshold = cfg.getInt("rows-per-key-threshold")

  /**
   * Builds a model with Apache Commons Math and returns (weights, intercept, r-squared)
   */
  def solveWithCommons(slice: RDD[LabeledPoint]): (Seq[Double], Double, Double) = {
    val y = slice.map(_.label).collect
    val x = slice.map(_.features.toArray).collect

    val regression = new OLSMultipleLinearRegression()
    regression.setNoIntercept(true)
    regression.newSampleData(y, x)

    (regression.estimateRegressionParameters, 0, regression.calculateRSquared)
  }

  /**
   * Builds a model with MLLib and returns (weights, intercept, r-squared)
   */
  def solveWithMLLib(config: RegressionConfig)(slice: RDD[LabeledPoint]): (Seq[Double], Double, Double) = {
    val model = buildModel(config, slice)

    val prediction = model predict (slice map (_.features))
    val predictionAndLabel = prediction zip (slice map (_ label))

    val meanLabel = slice map (_.label) mean
    val totSS = slice map (p => (p.label - meanLabel) * (p.label - meanLabel)) sum
    val resSS = predictionAndLabel map {
      case (pred, label) => (pred - label) * (pred - label)
    } sum
    val r2 = 1.0 - resSS / totSS

    (model.weights.toArray, model.intercept, r2)
  }

  /**
   * Builds the regression model.
   */
  protected def buildModel(config: RegressionConfig, slice: RDD[LabeledPoint]): GeneralizedLinearModel = {
    val (maxLabel, maxFeatures) = findMaxValues(slice)

    val scaledSlice = slice map { pt =>
      val label = pt.label / maxLabel
      val features = pt.features.toArray zip maxFeatures map (x => x._1 / x._2)
      LabeledPoint(label, Vectors.dense(features))
    }

    val scaledModel = config.algorithm.run(scaledSlice)
    val weights = scaledModel.weights.toArray zip maxFeatures map {
      case (w, scale) => w * maxLabel / scale
    }
    val intercept = scaledModel.intercept * maxLabel
    new LinearRegressionModel(Vectors.dense(weights), intercept)
  }

  /**
   * Finds the maximum magnitude for the label and features.
   */
  protected def findMaxValues(points: RDD[LabeledPoint]): (Double, IndexedSeq[Double]) = {
    def oneIfZero(value: Double) = if (value != 0.0) value else 1.0

    val maxLabel = oneIfZero(points map (_.label) map math.abs max)
    val size = points.first.features.size
    val maxFeatures = (0 until size) map { idx =>
      oneIfZero(points map (_.features(idx)) map math.abs max)
    }
    (maxLabel, maxFeatures)
  }
}