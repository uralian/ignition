package com.ignition

import org.specs2.mutable.Specification
import org.specs2.specification.{ Fragments, Step }

/**
 * Trait that provides hooks for executing tasks before an after *ALL* tests
 *  have started / ended.  This should be extend and the beforAll and afterAll
 *  functions overridden by the implementing class.
 *
 *  __Example:__
 *
 *  {{{
 *    class MyoSpec extends BeforeAndAfterAll {
 *
 *      override def beforeAll() {
 *        // do something before all tests
 *      }
 *
 *      override def afterAll() {
 *        // do something after all tests have completed
 *      }
 *
 *      ...
 *    }
 *  }}}
 *
 *  Taken from http://stackoverflow.com/questions/16936811/execute-code-before-and-after-specification
 *  @see http://bit.ly/11I9kFM (specs2 User Guide)
 */
trait BeforeAllAfterAll extends Specification {
  override def map(fragments: => Fragments) = Step(beforeAll) ^ fragments ^ Step(afterAll)

  protected def beforeAll() = {}
  protected def afterAll() = {}
}