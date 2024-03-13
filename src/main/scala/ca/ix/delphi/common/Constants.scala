package ca.ix.delphi.common

object Constants {

  val PROCESS_THREADS = 3
  val LIVY_URL = ""
  val CONFIG_PATH = "conf"
  //val PROCESS_DATE=
  //val PROCESS_HOUR=
  val THREAD_SLEEP = 10000
  val RUNNING = "RUNNING"
  val STARTING = "STARTING"
  val SCHEDULED = "SCHEDULED"
  val SUCCESS = "SUCCESS"
  val DEAD = "DEAD"

  val TABLE_INSTANCE_DETAILS = "job_instance_details"
  val TMP_INSTANCE_DETAILS = "tmp_job_instance_details"


}

object JobStatus extends Enumeration {
  type Status = Value
  val SUCCESS, RUNNING, DEAD = Value
}
