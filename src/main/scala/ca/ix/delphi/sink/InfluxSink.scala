package ca.ix.delphi.sink

import ca.ix.delphi.database.{DelphiInstance, DelphiSchedulerInstance}
import ca.ix.hubble.common.HubbleLogging
import ca.ix.hubble.common.influx.HubbleInfluxAPIUtil

import scala.collection.mutable.Map

object InfluxSink {

  val MEASUREMENT = "delphi_instance_status"

  def getInfluxStatusCode(status: String): Int = {

    status.toUpperCase match {
      case "SUCCESS"   => 0
      case "STARTING"  => 12
      case "RUNNING"   => 13
      case "DEAD"      => 101
      case "SCHEDULED" => 501

    }
  }

  def insertInfluxDataPointTags(delphiObj: DelphiSchedulerInstance): Map[String, String] = {

    val tags = Map[String, String]()
    tags("instanceID") = delphiObj.delphiInstanceID.toString
    tags("jobName") = delphiObj.jobName
    tags("jobDesc") = delphiObj.description.replace(" ", "\\ ")
    tags("jobPriority") = delphiObj.priority.toString
    tags("processDate") = delphiObj.processDate

    tags
  }

  def insertInfluxDataPointFields(status: String, time: Long): Map[String, Long] = {

    val currentTime = System.currentTimeMillis()
    val runTime = currentTime - time

    val fields = Map[String, Long]()
    fields("status") = getInfluxStatusCode(status)
    fields("runtime") = runTime
    fields
  }

  def generateInfluxDataPointTags(jobName: String, jobDesc: String): Map[String, String] = {

    val tags = Map[String, String]()
    tags("jobName") = jobName
    tags("jobDesc") = jobDesc.replace(" ", "\\ ")

    tags
  }
}
