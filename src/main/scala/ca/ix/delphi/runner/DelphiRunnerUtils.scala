package ca.ix.delphi.runner

import java.time.LocalDateTime
import java.io.File
import java.time.format.DateTimeFormatter

import ca.ix.delphi.database.{DelphiDBSqlUtils, DelphiInstance, DelphiSchedulerInstance}
import ca.ix.delphi.sink.InfluxSink
import ca.ix.hubble.common.HubbleLogging
import ca.ix.hubble.common.influx.HubbleInfluxAPIUtil
import com.typesafe.config.{Config, ConfigFactory, ConfigMergeable}

import scala.util.parsing.json.{JSON, JSONObject}
import scala.collection.JavaConverters._
import ca.ix.delphi.common.Constants._

object DelphiRunnerUtils extends HubbleLogging {

  /*  def getLivyArgs(cfg: Config): String = {}

  def getLivyConf(cfg: Config): String = {}

  def getLivyAPIJson(args: String, conf: String, config: Config): String = {}
  def getJobList(): List[DelphiSchedulerInstance] = {

    List("")
  }*/

  // TODO Move to read Hadoop Files
  def getFilesList(dir: String): List[File] = {
    val fileDir = new File(dir)
    if (fileDir.exists && fileDir.isDirectory) {
      fileDir.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getSchedulerInstanceList(lst: List[File], defaultJobConfig: Config, influxTime: String): List[DelphiInstance] = {

    lst.map { file =>
      val jobConfig = configFileParser(file)
        .withFallback(defaultJobConfig)

      val jobName = jobConfig.getString("name")
      val jobDesc = jobConfig.getString("desc")
      val jobPriority = jobConfig.getInt("priority")
      val jobScheduler = "spark"
      val jobFrequency = "daily"
      val jobCfgFileName = file.getName
      val apiJson = getRestAPIJson(jobConfig)

      DelphiInstance(jobName, jobDesc, jobPriority, jobScheduler, jobFrequency, jobCfgFileName, apiJson, currentDate, influxTime)
    }

  }

  def getRestAPIJson(jobConfig: Config): String = {

    val livyRequestMap = Map(
      "proxyUser" -> s"${jobConfig.getString("runUser")}",
      "file"      -> s"${jobConfig.getString("jarfile")}",
      "className" -> s"${jobConfig.getString("className")}",
      "queue"     -> s"${jobConfig.getString("queue")}",
      "name"      -> s"${jobConfig.getString("name")}"
    )

    val jobArgsJsonString = parseJobConfigToJson(jobConfig, "job-config")
    val sparkConfigJsonString = parseSparkConfigToJson(jobConfig, "spark-config")

    val livyRequestMapJsonString = JSONObject(livyRequestMap)
      .toString()
      .replace("\\", "")
      .replace("}", "")

    val restAPIRequestJson = (livyRequestMapJsonString + ", " + jobArgsJsonString + ", " + sparkConfigJsonString).replace("}", "}}")

    restAPIRequestJson
  }

  def getRestAPIJsonWithName(jobConfig: Config, name: String, explicitArgsString: String): String = {

    val livyRequestMap = Map(
      "proxyUser" -> s"${jobConfig.getString("runUser")}",
      "file"      -> s"${jobConfig.getString("jarfile")}",
      "className" -> s"${jobConfig.getString("className")}",
      "queue"     -> s"${jobConfig.getString("queue")}",
      "name"      -> s"$name"
    )

    val jobArgsJsonString = parseJobConfigToJsonDevRunner(jobConfig, "job-config", explicitArgsString)
    val sparkConfigJsonString = parseSparkConfigToJson(jobConfig, "spark-config")

    val livyRequestMapJsonString = JSONObject(livyRequestMap)
      .toString()
      .replace("\\", "")
      .replace("}", "")

    val restAPIRequestJson = (livyRequestMapJsonString + ", " + jobArgsJsonString + ", " + sparkConfigJsonString).replace("}", "}}")

    restAPIRequestJson
  }

  def parseJobConfigToJsonDevRunner(jobConfig: Config, key: String, explicitParams: String): String = {

    val argsString = jobConfig
      .getObject(key)
      .asScala
      .map {
        case (k, v) =>
          s""""--$k", ${v.render()}"""
      }
      .mkString(",")

    //["--output-dir", "/tmp/james/etl/hubble-insights/complete/cafemom-base-performance/parquet","--event-time-processing-id-combination",
    // "20191017203900:201809100700;20191017203900:201809100800","--base-mapping-dir", "/user/hubble-dev/data-mappings","--processing-id",
    // "20190101200000","--input-dir", "/tmp/transformer_partner_id/"]

    val finalString = s""""args":[$argsString,$explicitParams]"""
    finalString
  }

  def parseJobConfigToJson(jobConfig: Config, key: String): String = {

    val argsString = jobConfig
      .getObject(key)
      .asScala
      .map {
        case (k, v) =>
          s""""--$k", ${v.render()}"""
      }
      .mkString(",")

    //["--output-dir", "/tmp/james/etl/hubble-insights/complete/cafemom-base-performance/parquet","--event-time-processing-id-combination",
    // "20191017203900:201809100700;20191017203900:201809100800","--base-mapping-dir", "/user/hubble-dev/data-mappings","--processing-id",
    // "20190101200000","--input-dir", "/tmp/transformer_partner_id/"]

    val finalString = s""""args":[$argsString]"""
    finalString
  }

  def parseSparkConfigToJson(jobConfig: Config, key: String): String = {

    val sparkconfigMap = jobConfig
      .getObject(key)
      .asScala
      .map {
        case (k, v) =>
          (s"$k" -> v.render)
      }
      .toMap

    // {"spark.network.timeout" : "600", "spark.driver.memory" : "12g", "spark.executor.instances" : "10",
    // "spark.sql.codegen.wholeStage" : "false", "spark.sql.broadcastTimeout" : "600", "spark.executor.memory" : "12g",
    // "spark.executor.cores" : "4", "spark.yarn.executor.memoryOverhead" : "2g"}

    val finalString = s""""conf":${JSONObject(sparkconfigMap).toString().replace("\\\"", "")}"""
    finalString

  }

  def configFileParser(file: File): Config = {
    ConfigFactory.parseFile(file)
  }

  /** currentTimeStamp provides current time in timestamp format.
    *
    * @return
    */
  def currentTimeStamp: String = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now)

  def currentDate: String = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now)

  /** printString function will print the passed String to stdout with timestamp and class Name format.
    *
    * @param str Input String to be logged to std out.
    */
  def printString(str: String): Unit = {

    val className = this.getClass.getName.stripSuffix("$").split('.').last
    println(s"$currentTimeStamp $className: $str")
  }

  /** to parser the json dat return from Rest API
    *
    * @param jsonString
    * @return
    */
  def parseJsonData(jsonString: String): Map[String, Any] = {
    val parserPostResponse = JSON.parseFull(jsonString)
    val map: Map[String, Any] = parserPostResponse.get.asInstanceOf[Map[String, Any]]

    map
  }

  def createInfluxDBInstance(delphiConfig: Config): HubbleInfluxAPIUtil = {
    val influxDB = delphiConfig.getString("influx.database")
    val influxConf = delphiConfig.getString("influx.cfg")

    new HubbleInfluxAPIUtil(influxConf, influxDB)
  }

  def initializeDatabase(dt: String, lst: List[File], config: Config, dbUtil: DelphiDBSqlUtils, influxTime: String): Unit = {

    printString("Initializing Database for the day")
    val DBInstanceList = getSchedulerInstanceList(lst, config, influxTime)

    //load to tmp table
    printString("Loading to TMP table..")
    dbUtil.truncateTMPTable(TMP_INSTANCE_DETAILS)

    DBInstanceList
      .foreach { t =>
        dbUtil.createDelphiInstance(t, TMP_INSTANCE_DETAILS)
      }

    //check Failed and update from tmp if already ran for today.
    val firstRunFLag = dbUtil.checkIfFirstRun(currentDate)

    if (firstRunFLag) {
      printString("Loading Configurations to the Instance table..")
      DBInstanceList
        .foreach { t =>
          dbUtil.createDelphiInstance(t, TABLE_INSTANCE_DETAILS)
        }
    }
    else {
      printString("Database already Initialized for the day")
      printString("Updating Failed Json Config in Instance Table Only..")

      dbUtil.updateFailedJsonString(currentDate)
    }
  }

  def updateInfluxDBStatusInstance(t: DelphiSchedulerInstance, hubbleInfluxSink: HubbleInfluxAPIUtil, measureName: String, status: String, time: Long): Unit = {

    val tags = InfluxSink.insertInfluxDataPointTags(t)
    val fields = InfluxSink.insertInfluxDataPointFields(status,time)
    hubbleInfluxSink.insertPoint(measureName, tags, fields, time)

  }

  def updateInfluxDBStatusInstanceDevRunner(jobName: String,
                                            jobDesc: String,
                                            hubbleInfluxSink: HubbleInfluxAPIUtil,
                                            measureName: String,
                                            status: String,
                                            time: Long): Unit = {

    val tags = InfluxSink.generateInfluxDataPointTags(jobName, jobDesc)
    val fields = InfluxSink.insertInfluxDataPointFields(status,time)
    hubbleInfluxSink.insertPoint(measureName, tags, fields, time)

  }

}
