package ca.ix.delphi.operators

import com.typesafe.config.Config

import scala.concurrent.Future
import scala.collection.parallel.ParSeq

trait SchedulerService {

  /**
    * submitJob will submit the job to a given scheduler.
    *
    * @param livyAPIJson livyAPIJson String for the job submission.
    * @return
    */
  def submitJob(livyAPIJson: String): String

}
