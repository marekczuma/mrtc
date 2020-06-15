package models

import java.util.Calendar

/**
 * Model of Step.
 * @param direction - N, S, W or E.
 * @param timeToWait - If rover must wait, here we save time (in minutes) to wait.
 * @param isActive - At the same time rover can have only one active step.
 */
class Step(val direction: String, var timeToWait: Int, var isActive: Boolean = false) {
  var startTime: Long = 0

  /**
   * Time begins to be counted from when we call this method.
   */
  def activateStep(): Unit ={
    isActive = true
    startTime = Calendar.getInstance().getTimeInMillis
  }

  def deactivateStep(): Unit ={
    isActive = false
  }
}
