package models

import org.scalatest.FlatSpec

class StepSpec extends FlatSpec{

  "A step" should "activate itself" in {
    val step = new Step("W", 2)
    val previousStartTime = step.startTime
    step.activateStep()
    assert(step.isActive)
    assert(step.startTime > previousStartTime)
  }
}
