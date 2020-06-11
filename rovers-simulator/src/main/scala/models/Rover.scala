package models

import java.util.Calendar

import hbase.GlobalRepository
import services.RoversManager

import scala.collection.mutable.ListBuffer

class Rover(val id: Int, var currentCoords: ListBuffer[Int]) {
  var currentPlan: Plan = new Plan()
  sendCoordsToHBase()

  def runIfPossible(): Unit ={
    try {
      if(currentPlan.isApproved){
        val step: Step = currentPlan.activeStep()
        val duration = Calendar.getInstance().getTimeInMillis - step.startTime
        if (duration >= 60000 * step.timeToWait) { // 60 000 ms = 1min
          run(step.direction)
          currentPlan.activateNextStep()
        }
      }

    }catch {
      case e: Exception => {
        generateNewPlan()
        sendQuestionWithPlan()
      }
    }
  }

  def run(direction: String): Unit ={
    direction.toUpperCase match {
      case "N" => currentCoords(1) += 1
      case "S" => currentCoords(1) -= 1
      case "E" => currentCoords(0) += 1
      case "W" => currentCoords(0) -= 1
    }
    println(s"${Calendar.getInstance().getTime} ROVER ID: ${id}; MOVE: ${direction.toUpperCase}; COORDINATES: ${currentCoords}")
    sendCoordsToHBase()
  }

  def approvePlan(times: String): Unit = {
    val timesList: ListBuffer[Int] = ListBuffer(times.split(",").map(t=> t.toInt): _*)
    currentPlan.steps.zipWithIndex.foreach(s => s._1.timeToWait = timesList(s._2))
    currentPlan.steps.head.activateStep()
    currentPlan.isApproved = true
    println(s"${Calendar.getInstance().getTime} ROVER ID: ${id}; NEW PLAN APPROVED")
    val coordsForUpdate = changeStepsToCoords(currentPlan.steps).zipWithIndex.map(s=> (s"step-${s._2+1}"->s._1)).toMap
    val timesForUpdate = changeStepToTimestampInMillisecs(currentPlan.steps).zipWithIndex.map(s=> (s"time-${s._2+1}"->s._1)).toMap
    GlobalRepository.putRowToTable(id.toString, coordsForUpdate.++(timesForUpdate))
  }

  def generateNewPlan(): Unit ={
    currentPlan = new Plan()
    println(s"${Calendar.getInstance().getTime} ROVER ID: ${id}; NEW PLAN GENERATED")
  }

  def sendQuestionWithPlan(): Unit ={
    val stepsDirections: String = currentPlan.steps.map(s=> s.direction).mkString("")
    val coordsString: String = currentCoords.map(c=> c.toString).mkString(",")
    val question = s"${id}-${coordsString}-${stepsDirections}"
    RoversManager.questions += question
    println(s"${Calendar.getInstance().getTime} ROVER ID: ${id}; NEW QUESTION ADDED")
  }

  def sendCoordsToHBase(): Unit ={
    val coordsForUpdate = Map("coords"->currentCoords.mkString(","))
    GlobalRepository.putRowToTable(id.toString, coordsForUpdate)
  }

  def changeStepsToCoords(steps: ListBuffer[Step]): ListBuffer[String]={
    val tmpCoord = currentCoords
    for(d<- steps.map(dir=> dir.direction)) yield {
      d.toUpperCase match {
        case "N" => tmpCoord(1) += 1
        case "S" => tmpCoord(1) -= 1
        case "E" => tmpCoord(0) += 1
        case "W" => tmpCoord(0) -= 1
      }
      s"${tmpCoord(0)},${tmpCoord(1)}"
    }
  }

  def changeStepToTimestampInMillisecs(steps: ListBuffer[Step]): ListBuffer[String]={
    var tmpTime = System.currentTimeMillis()
    for(t<-steps.map(s=>s.timeToWait)) yield {
      tmpTime = tmpTime + (t+2)*60000
      tmpTime.toString
    }
  }

}
