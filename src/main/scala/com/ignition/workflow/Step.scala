package com.ignition.workflow

import scala.reflect.{ ClassTag, classTag }

/**
 * Workflow step. Evaluates in an execution context E to a value of type T.
 *
 * @author Vlad Orzhekhovskiy
 */
sealed trait Step[T, E] {
  implicit def outType: ClassTag[T] = classTag[T]

  def output(implicit ec: E): T

  protected def inputValue[S](src: Option[Step[S, E]], name: String)(implicit ec: E): S =
    src map (_.output) getOrElse (throw WorkflowException(s"$name is not connected"))
}

/**
 * Supplies the method for creating outbound connections.
 */
sealed trait ConnectOut[T, E, X <: Step[T, E]] { self: X =>
  def connectTo[TT](target: Step1[T, TT, E]) = { target.connectFrom(this); target }
  def connectTo1[T2, TT](target: Step2[T, T2, TT, E]) = { target.connect1From(this); target }
  def connectTo2[T2, TT](target: Step2[T2, T, TT, E]) = { target.connect2From(this); target }
  def connectTo[TT](target: StepN[T, TT, E]) = { target.connectFrom(this); target }
}

/**
 * Workflow step without any inputs.
 */
abstract class Step0[T: ClassTag, E](func: E => T) extends Step[T, E] with ConnectOut[T, E, Step0[T, E]] {
  def output(implicit ec: E) = func(ec)
}

/**
 * Workflow step with one input.
 */
abstract class Step1[S: ClassTag, T: ClassTag, E](func: E => S => T) extends Step[T, E] with ConnectOut[T, E, Step1[S, T, E]] {
  private var in: Option[Step[S, E]] = None

  def connectFrom(source: Step[S, E]) = { in = Some(source) }
  def output(implicit ec: E) = func(ec)(inputValue(in, "Input"))
}

/**
 * Workflow step with two inputs.
 */
abstract class Step2[S1: ClassTag, S2: ClassTag, T: ClassTag, E](func: E => (S1, S2) => T)
  extends Step[T, E] with ConnectOut[T, E, Step2[S1, S2, T, E]] {

  var in1: Option[Step[S1, E]] = None
  var in2: Option[Step[S2, E]] = None

  def connect1From(source: Step[S1, E]) = { in1 = Some(source) }
  def connect2From(source: Step[S2, E]) = { in2 = Some(source) }

  def output(implicit ec: E) = func(ec)(inputValue(in1, "Input1"), inputValue(in2, "Input2"))
}

/**
 * Workflow step with arbitrary number of inputs of the same type.
 */
abstract class StepN[S: ClassTag, T: ClassTag, E](func: E => Iterable[S] => T)
  extends Step[T, E] with ConnectOut[T, E, StepN[S, T, E]] {

  val ins = collection.mutable.ArrayBuffer[Step[S, E]]()

  def connectFrom(source: Step[S, E]) = { ins += source }

  def output(implicit ec: E) = func(ec)(ins map (_.output))
}