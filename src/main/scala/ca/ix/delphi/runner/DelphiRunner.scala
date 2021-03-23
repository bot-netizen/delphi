package ca.ix.delphi.runner

import ca.ix.hubble.common.HubbleLogging
import ca.ix.delphi.runner.DelphiRunnerUtils.{getRestAPIJson, _}
import ca.ix.delphi.common.Constants._
import java.io.File

import ca.ix.delphi.database.DelphiDBSqlUtils
import ca.ix.delphi.operators.SparkLivyScheduler

object DelphiRunner extends HubbleLogging {

  def main(args: Array[String]): Unit = {

    val delphiConfig = configFileParser(new File("conf/delphi/delphi-daily.dev.conf"))
    val defaultConfig = configFileParser(new File("conf/decepticons/default/application.dev.conf"))

    val dbUtil = DelphiDBSqlUtils.createDBConnection(delphiConfig.getString("configDB.cfg"))
    val influxMeasurement = delphiConfig.getString("influx.measurement")
    val scheduler = new SparkLivyScheduler
    val influxSink = createInfluxDBInstance(delphiConfig)
    val influxTime = System.currentTimeMillis().toString

    //try {

    val fileList = getFilesList("conf/decepticons").filter(t => t.getName.endsWith("dev.conf"))
    initializeDatabase(currentDate, fileList, defaultConfig, dbUtil, influxTime)

    // Get List of jobs to be processed today.
    val schedulerInstanceList = dbUtil.getJobsListToExecute(currentDate)
    schedulerInstanceList.foreach { t =>
      updateInfluxDBStatusInstance(t, influxSink, influxMeasurement, SCHEDULED, t.influxTime.toLong)
    }

    val schedulerPool = java.util.concurrent.Executors.newFixedThreadPool(PROCESS_THREADS)


    schedulerInstanceList
      .foreach { t =>
        schedulerPool
          .execute(new Runnable {
            def run {
              printString(s"Running Job : ${t.jobName}  Thread Pool : ${Thread.currentThread().getName}")
              scheduler.executeJob(t, dbUtil, influxSink, influxMeasurement, t.influxTime.toLong)
            }
          })
      }
  }
}
