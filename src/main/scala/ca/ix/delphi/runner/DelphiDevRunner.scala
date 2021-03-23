package ca.ix.delphi.runner

import java.io.File

import ca.ix.delphi.operators.SparkLivyScheduler
import ca.ix.delphi.runner.DelphiRunnerUtils._
import ca.ix.hubble.common.{DelphiCLIParser, HubbleLogging}
import com.typesafe.config.Config

object DelphiDevRunner extends DelphiCLIParser with HubbleLogging {

  def getJobConfig(cfgFile: String, jobName: String): (Config, String) = {

    (cfgFile.isEmpty, jobName.isEmpty) match {
      case (true, false) => (configFileParser(new File("conf/runners/decepticons-runner.conf")), jobName)
      case (false, true) =>
        val jobConfig = configFileParser(new File(cfgFile))
          .withFallback(configFileParser(new File("conf/runners/decepticons-runner.conf")))
        val jobName = jobConfig.getString("job-name")
        (jobConfig, jobName)
      case _ => null
    }
  }

  def getParamString(lst: List[(String, String)]): String = {

    lst
      .map { t =>
        s""""${t._1}","${t._2}""""
      }
      .mkString(",")
  }

  def main(args: Array[String]): Unit = {

    // Delphi Configuration
    val delphiConfig = configFileParser(new File("conf/delphi/delphi-dev-runner.conf"))
    val influxMeasurement = delphiConfig.getString("influx.measurement")
    val scheduler = new SparkLivyScheduler
    val influxSink = createInfluxDBInstance(delphiConfig)
    val influxTime = System.currentTimeMillis().toString

    // Check if user passed CFG or the job Name. with job Name all will be default
    parseCLIArguments(args)
    val (conf, job) = getJobConfig(configFIle, jobName)
    val jobDescription = if (jobDesc.isEmpty) conf.getString("desc") else jobDesc

    val outputBase = conf.getString("base-output-dir")
    val outputDir = outputBase + "/" + jobName
    val outputDirCsv = outputDir + "/csv"
    val paramList = List(("--job-name", jobName), ("--output-dir", outputDir), ("--output-dir-csv", outputDirCsv))
    val paramString = getParamString(paramList)

    val restRequest = getRestAPIJsonWithName(conf, job, paramString)

    printString(s"Scheduling Job $jobName => $jobDescription")
    scheduler.executeJobDevRunner(job, jobDescription, restRequest, influxSink, influxMeasurement, influxTime.toLong)

  }
}
