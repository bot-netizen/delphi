package ca.ix.delphi.operators

import ca.ix.hubble.common.HubbleLogging
import scalaj.http.Http
import ca.ix.delphi.common.Constants._
import ca.ix.delphi.database.{DelphiDBSqlUtils, DelphiSchedulerInstance}
import ca.ix.delphi.runner.DelphiRunnerUtils._
import ca.ix.hubble.common.influx.HubbleInfluxAPIUtil

class SparkLivyScheduler extends SchedulerService with HubbleLogging {

  override def submitJob(livyAPIJson: String): String = {

    Http(LIVY_URL)
      .postData(livyAPIJson)
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .asString
      .body
  }

  def getJobStatus(appID: Long, t: DelphiSchedulerInstance, hubbleInfluxSink: HubbleInfluxAPIUtil, measureName: String, time: Long): String = {

    var jobFinishedFlag = false
    var jobStatus = RUNNING
    var updateRunningStatusFlag = true

    try {

      while (!jobFinishedFlag) {
        Thread.sleep(THREAD_SLEEP)

        jobStatus = parseJobStateFromResponse(appID).toUpperCase

        if (jobStatus == RUNNING && updateRunningStatusFlag) {
          updateInfluxDBStatusInstance(t, hubbleInfluxSink, measureName, RUNNING, time)
          updateRunningStatusFlag = false
        }
        jobFinishedFlag = getJobFinishedFlag(jobStatus)
      }
      jobStatus

    } catch {
      case e: Exception =>
        if (e.getMessage.contains("connect timed out")) {
          getJobStatus(appID, t, hubbleInfluxSink, measureName, time)
        } else {
          null
        }
    }
  }

  def getJobStatusDevRunner(appID: Long, jobName: String, jobDesc: String, hubbleInfluxSink: HubbleInfluxAPIUtil, measureName: String, time: Long): String = {

    var jobFinishedFlag = false
    var jobStatus = RUNNING
    var updateRunningStatusFlag = true

    printString("Job Status Changed to Starting..")

    try {

      while (!jobFinishedFlag) {
        Thread.sleep(THREAD_SLEEP)

        jobStatus = parseJobStateFromResponse(appID).toUpperCase

        if (jobStatus == RUNNING && updateRunningStatusFlag) {
          printString("Job Status Changed to Running..")
          updateInfluxDBStatusInstanceDevRunner(jobName, jobDesc, hubbleInfluxSink, measureName, RUNNING, time)
          updateRunningStatusFlag = false
        }
        jobFinishedFlag = getJobFinishedFlag(jobStatus)
      }
      printString(s"Job Status Changed to ${jobStatus}..")

      jobStatus

    } catch {
      case e: Exception =>
        if (e.getMessage.contains("connect timed out")) {
          getJobStatusDevRunner(appID, jobName, jobDesc, hubbleInfluxSink, measureName, time)
        } else {
          null
        }
    }
  }

  private def getJobFinishedFlag(status: String): Boolean = {
    status match {
      case RUNNING        => false
      case SUCCESS | DEAD => true
      case _              => false
    }
  }

  private def getLivyResponseJson(appID: Long): String = {
    Http(LIVY_URL + appID).asString.body
  }

  private def parseJobStateFromResponse(appID: Long): String = {
    parseJsonData(getLivyResponseJson(appID))("state").toString
  }

  private def parseSparkURLFromResponse(appID: Long): String = {
    parseJsonData(getLivyResponseJson(appID))("appInfo").toString
  }

  private def parseAppIDFromResponse(json: String): Long = {
    parseJsonData(json)("id").toString.toDouble.toLong
  }

  def executeJob(t: DelphiSchedulerInstance, dbUtil: DelphiDBSqlUtils, hubbleInfluxSink: HubbleInfluxAPIUtil, measureName: String, time: Long): Unit = {

    try {
      val result = submitJob(t.restAPIJson)
      val appID = parseAppIDFromResponse(result)

      dbUtil.insertJobStatus(t.delphiInstanceID, appID.toString, STARTING)
      updateInfluxDBStatusInstance(t, hubbleInfluxSink, measureName, STARTING, time)

      val exitStatus = getJobStatus(appID, t, hubbleInfluxSink, measureName, time)

      updateInfluxDBStatusInstance(t, hubbleInfluxSink, measureName, exitStatus, time)
      dbUtil.insertJobStatus(t.delphiInstanceID, appID.toString, exitStatus)

      printString(s"${t.jobName} Status: " + exitStatus)

    } catch {
      case _: Exception => null
    }
  }

  def executeJobDevRunner(jobName: String, jobDesc: String, restAPIJson: String, hubbleInfluxSink: HubbleInfluxAPIUtil, measureName: String, time: Long): Unit = {

    try {
      val result = submitJob(restAPIJson)
      val appID = parseAppIDFromResponse(result)

      updateInfluxDBStatusInstanceDevRunner(jobName, jobDesc, hubbleInfluxSink, measureName, STARTING, time)
      printString(s"Livy Url:  ${LIVY_URL}")
      printString(s"Livy App ID: ${appID}")

      val exitStatus = getJobStatusDevRunner(appID, jobName, jobDesc, hubbleInfluxSink, measureName, time)
      updateInfluxDBStatusInstanceDevRunner(jobName, jobDesc, hubbleInfluxSink, measureName, exitStatus, time)
      printString(s"${jobName} Status: " + exitStatus)

    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

}
