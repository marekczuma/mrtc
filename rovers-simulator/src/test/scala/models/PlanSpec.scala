package models

import org.scalatest.FlatSpec

import scala.collection.mutable.ListBuffer

class PlanSpec extends FlatSpec {

  "A Plan" should "have second step as active" in {
    val plan = new Plan()
    plan.steps.head.activateStep()
    plan.activateNextStep()
    assert(plan.steps.indexOf(plan.activeStep()).equals(1))
  }
}
