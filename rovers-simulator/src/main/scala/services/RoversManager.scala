package services

import java.util.Calendar

import kafka.QuestionsStreaming
import models.Rover

import scala.collection.mutable.ListBuffer

/**
 * Class for rovers managment. RM sends questions to kafka topic and sends answers from kafka to rovers.
 * Also here we have "runTheWorld()" method that has invinite loop and checks possibility to move by rovers.
 */
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
      val id: String = a.split("-").head
      rovers.find(r=>
        r.id.equals(id.toInt)).get.approvePlan(a.split("-")(1))
    })
  }

  /**
   * Rover can use this method to add question to collection.
   * Every 5 seconds questions are sending to kafka "questions" topic.
   */
  def sendQuestions(): Unit ={
    val duration: Long = Calendar.getInstance().getTimeInMillis - timerInMillis
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
    val tmpRovers: ListBuffer[Rover] = ListBuffer((1 to 50).map(i=> new Rover(i,ListBuffer(Seq(1000+i,1000): _*))): _*)
    tmpRovers.foreach(r=> r.sendQuestionWithPlan())
    tmpRovers
  }
}
