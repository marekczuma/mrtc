package models

import java.util.Calendar

class Step(val direction: String, var timeToWait: Int, var isActive: Boolean = false) {
  var startTime: Long = 0

  def activateStep(): Unit ={
    isActive = true
    startTime = Calendar.getInstance().getTimeInMillis
  }

  def deactivateStep(): Unit ={
    isActive = false
  }
}
