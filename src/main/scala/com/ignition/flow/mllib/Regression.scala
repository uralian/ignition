package com.ignition.flow.mllib

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.optimization._
import org.apache.spark.sql.{ DataFrame, SQLContext, Row }
import org.apache.spark.sql.types.StructType
import org.apache.spark.rdd.RDD

import com.ignition.flow.Transformer
import com.ignition.types._
import com.ignition.util.ConfigUtils._
import com.ignition.SparkRuntime

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression

/**
 * Regression methods.
 */
object RegressionMethod extends Enumeration {
  trait RegressionMethod[T <: GeneralizedLinearModel] extends super.Val {
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
}

/**
 * Computes the regression using MLLib library.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Regression(labelField: String, dataFields: Iterable[String] = Nil,
  groupFields: Iterable[String] = Nil, config: RegressionConfig = RegressionConfig())
  extends Transformer with MLFunctions {

  import Regression._

  def label(field: String) = copy(labelField = field)
  def columns(fields: String*) = copy(dataFields = fields)
  def groupBy(fields: String*) = copy(groupFields = fields)

  def method(method: RegressionMethod[_ <: GeneralizedLinearModel]) =
    copy(config = config.method(method))
  def iterations(num: Int) = copy(config = config.iterations(num))
  def step(num: Double) = copy(config = config.step(num))
  def intercept(flag: Boolean) = copy(config = config.intercept(flag))

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val df = optLimit(arg, limit)

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

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType =
    compute(input(Some(100)), Some(100)) schema

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Regression companion object.
 */
object Regression {

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