package ca.ix.hubble.common

/** CLI parser class is used to parse the command line arguments for the spark jobs
  *
  */

class DelphiCLIParser extends java.io.Serializable {

  var jobName: String = _
  var configFIle: String = _
  var slackFLag: Boolean = _
  var jobDesc: String = _

  /** Command Line Config Class passes the workflow parameters to the Scopt.
    *
    * @param jobName          Job Name for the job
    * @param configFIle       Configuration name for the job config.
    */
  case class CommandLineConfig(
      jobName: String = "",
      configFIle: String = "",
      slackFLag: Boolean = false,
      jobDesc: String = ""
  )

  val parser = new scopt.OptionParser[CommandLineConfig]("delphi") {
    opt[String]("job-name")
      .optional()
      .action { (x, c) =>
        c.copy(jobName = x)
      } text "Job Name for execution."
    opt[String]("config-file")
      .optional()
      .action { (x, c) =>
        c.copy(configFIle = x)
      } text "Job Configuration file for scheduling."
    opt[String]("job-desc")
      .optional()
      .action { (x, c) =>
        c.copy(configFIle = x)
      } text "Job Description for scheduling."
    opt[Boolean]("slack-notification")
      .optional()
      .action { (x, c) =>
        c.copy(slackFLag = x)
      } text "Slack Notification flag"
  }



  def parseCLIArguments(args: Array[String]): Unit = {

    parser.parse(args, CommandLineConfig()) match {
      case Some(config) =>
        this.jobName = config.jobName
        this.configFIle = config.configFIle
        this.jobDesc = config.jobDesc
        this.slackFLag = config.slackFLag

      case None =>
        None
    }
  }
}
