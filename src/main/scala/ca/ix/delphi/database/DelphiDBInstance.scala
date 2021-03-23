package ca.ix.delphi.database

// Case Class for Database Inserts.
case class DelphiInstance(jobName: String,
                          description: String,
                          priority: Int,
                          scheduler: String,
                          frequency: String,
                          cfgFileName: String,
                          restAPIJson: String,
                          processDate: String,
                          influxTimestamp: String)

// Case Class for Job Execution.
// Add status, start time, end time, run time on the fly.
case class DelphiSchedulerInstance(delphiInstanceID: Int,
                                   jobName: String,
                                   description: String,
                                   priority: Int,
                                   processDate: String,
                                   restAPIJson: String,
                                   influxTime: String)
