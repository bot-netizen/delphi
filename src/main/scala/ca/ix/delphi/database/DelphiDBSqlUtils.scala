package ca.ix.delphi.database

import com.typesafe.config.{Config, ConfigFactory}
import java.io.File
import ca.ix.hubble.common.sql.SqlService
import ca.ix.delphi.common.Constants._

object DelphiDBSqlUtils {

  def createDBConnection(conf: String): DelphiDBSqlUtils = {
    val dataConfig = ConfigFactory.parseFile(new File(conf))
    createDBConnection(dataConfig)
  }

  def createDBConnection(dataConfig: Config, shouldThrow: Boolean = true): DelphiDBSqlUtils = {
    val dbUtil = new DelphiDBSqlUtils(
      dataConfig.getString("db.host"),
      dataConfig.getString("db.port"),
      dataConfig.getString("db.username"),
      dataConfig.getString("db.password"),
      dataConfig.getString("db.database")
    )

    dbUtil.connect(shouldThrow)
    dbUtil
  }
}

class DelphiDBSqlUtils(val host: String,
                       val port: String,
                       val username: String,
                       val password: String,
                       val dbName: String,
                       val debug: Boolean = false,
                       val maxRetries: Int = 3,
                       val retryInterval: Int = 5)
    extends SqlService {

  val url = s"jdbc:mysql://$host:$port/$dbName?useSSL=false"

  val TABLE_INSTANCE_STATUS = "job_instance_status"
  val VW_DELPHI_LATEST_STATUS = "delphi_latest_job_status"

  val ID = "id"
  val JOB_NAME = "job_name"
  val JOB_DESC = "job_desc"
  val JOB_PRIORITY = "job_priority"
  val JOB_SCHEDULER = "job_scheduler"
  val JOB_FREQUENCY = "job_frequency"
  val CFG_NAME = "cfg_name"
  val JSON_STRING = "json_string"
  val PROCESS_DATE = "process_date"
  val INFLUX_TIMESTAMP = "influx_timestamp"

  val INSTANCE_ID = "instance_id"
  val APP_ID = "application_id"
  val STATUS = "status"

  def truncateTMPTable(tblName:String): Unit ={
      val deleteStatement = prepareStatement(s"""Delete from ${tblName}""")
      executeUpdateStatement(deleteStatement,null, commit = true, throwsException = true)

  }

  def createDelphiInstance(t: DelphiInstance, tblName: String): Unit = {

    val COL_COUNT = 9
    val delphiInstanceStatement = prepareStatement(
      s"""INSERT INTO ${tblName}($JOB_NAME, $JOB_DESC, $JOB_PRIORITY, $JOB_SCHEDULER, $JOB_FREQUENCY, $CFG_NAME, $JSON_STRING, $PROCESS_DATE, $INFLUX_TIMESTAMP)
         | VALUES (${buildQuestionMarkString(COL_COUNT)})""".stripMargin,
      s => {
        s.setString(1, t.jobName)
        s.setString(2, t.description)
        s.setInt(3, t.priority)
        s.setString(4, t.scheduler)
        s.setString(5, t.frequency)
        s.setString(6, t.cfgFileName)
        s.setString(7, t.restAPIJson)
        s.setString(8, t.processDate)
        s.setString(9, t.influxTimestamp)

      }
    )
    executeUpdateStatement(delphiInstanceStatement, null, commit = true, throwsException = true)
  }

  def updateFailedJsonString(date:String):Unit ={

    val jobStatusStatement = prepareStatement(
      s"""UPDATE ${TABLE_INSTANCE_DETAILS} dtl
         | join delphi_update_dead_json_string jsn
         | on dtl.ID = jsn.ID
         | SET dtl.json_string = jsn.json_string
         |Where dtl.process_date = '${date}'""".stripMargin
    )
    executeUpdateStatement(jobStatusStatement,null,commit = true, throwsException = true)
  }


  def insertJobStatus(instanceID: Int, appID: String, status: String): Unit = {

    val COL_COUNT = 3

    val jobStatusStatement = prepareStatement(
      s"""INSERT INTO ${TABLE_INSTANCE_STATUS}($INSTANCE_ID, $APP_ID, $STATUS)
         | VALUES (${buildQuestionMarkString(COL_COUNT)})""".stripMargin,
      s => {
        s.setInt(1, instanceID)
        s.setString(2, appID)
        s.setString(3, status)

      }
    )
    executeUpdateStatement(jobStatusStatement, null, commit = true, throwsException = true)
  }


  def checkIfFirstRun(date: String): Boolean = {

    val jobStatusStatement = prepareStatement(
      s"""SELECT COUNT(1) FROM ${TABLE_INSTANCE_DETAILS}
         |Where process_date = '${date}'""".stripMargin
    )
    flush()
    executeQueryStatement(jobStatusStatement, rs => rs.getInt(1) < 1, throwsException = true).head
  }


  def getJobsListToExecute(processDate: String): Seq[DelphiSchedulerInstance] = {

    val instanceStatement = prepareStatement(
      s"""
         SELECT ${ID}, ${JOB_NAME}, ${JOB_DESC}, ${JOB_PRIORITY}, ${PROCESS_DATE}, ${JSON_STRING}, ${INFLUX_TIMESTAMP}
         FROM ${TABLE_INSTANCE_DETAILS} dtl
         LEFT JOIN ${VW_DELPHI_LATEST_STATUS} st
         ON dtl.ID = st.instance_id
         WHERE ${PROCESS_DATE} = ?
         AND (${STATUS} is null or $STATUS = 'DEAD')
         ORDER BY ${JOB_PRIORITY} desc""",
      s => {
        s.setString(1, processDate)
      }
    )

    executeQueryStatement(
      instanceStatement,
      rs => DelphiSchedulerInstance(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getInt(4), rs.getString(5), rs.getString(6), rs.getString(7)),
      throwsException = true
    )
  }
}
