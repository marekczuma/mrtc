package services

import java.util.Calendar

import kafka.QuestionsStreaming
import models.Rover

import scala.collection.mutable.ListBuffer

object RoversManager {
  val questions: ListBuffer[String] = new ListBuffer[String]()
  val rovers: ListBuffer[Rover] = generateRovers()
  var timerInMillis: Long = Calendar.getInstance().getTimeInMillis

  def runTheWorld(): Unit ={
    while(true){
      rovers.foreach(rover=>{
        rover.runIfPossible()
      })
      sendQuestions()
    }
  }

  def sendAnswers(answers: ListBuffer[String]): Unit ={
    answers.foreach(a=> {
      val id = a.split("-").head
      rovers.find(r=>
        r.id.equals(id.toInt)).get.approvePlan(a.split("-")(1))
    })
  }

  def sendQuestions(): Unit ={
    val duration = Calendar.getInstance().getTimeInMillis - timerInMillis
    if(duration >= 5000 && questions.nonEmpty){
      println(s"${Calendar.getInstance().getTime} QUESTIONS SENDING START...")
      QuestionsStreaming.run(questions)
      questions.clear()
      timerInMillis = Calendar.getInstance().getTimeInMillis
      println(s"${Calendar.getInstance().getTime} QUESTIONS SENT")
    }else if(duration >= 5000){
      timerInMillis = Calendar.getInstance().getTimeInMillis
    }
  }

  def generateRovers(): ListBuffer[Rover]={
    val tmpRovers = ListBuffer((1 to 50).map(i=> new Rover(i,ListBuffer(Seq(1000+i,1000): _*))): _*)
    tmpRovers.foreach(r=> r.sendQuestionWithPlan())
    tmpRovers
  }
}
