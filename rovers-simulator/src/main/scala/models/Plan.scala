package models

import scala.collection.mutable.ListBuffer

class Plan {
  val steps: ListBuffer[Step] = prepareRandomSteps()
  var isApproved: Boolean = false

  def activateNextStep(): Unit ={
    val index = steps.indexOf(activeStep())
    steps(index).deactivateStep()
    if(index < steps.size - 1){
      steps(index + 1).activateStep()
    }
  }

  def activeStep(): Step={
    steps.filter(d=> d.isActive).head
  }

  def prepareRandomSteps(): ListBuffer[Step]={
    val listOfSteps: ListBuffer[String] = ListBuffer(Seq("N","S","W","E"): _*)
    ListBuffer((1 to 5).map(i=> new Step(listOfSteps(scala.util.Random.nextInt(3)), 0)): _*)
  }
}
