# Delphi - Job Scheduler

Delphi is a POC project for easing the job scheduling process for hourly/daily/Monthly and adhoc platonic table jobs. 
Platonic tables are the last component in the ETL pipeline and most of the jobs are simple aggregation queries without any join or explicit transformations.

There are many schedulers out there Airflow, Azkaban, Oozie and crontab etc. and all of them are great for scheduling the complex data pipeline jobs with complex interdependency's. 
We had a special case for scheduling jobs which is more focused on Adhoc or regularly scheduled data pulls.


**Delphi Features**:

* Priority-based scheduling.
* Low Dev Overhead for adding new jobs.
* Transparency on Scheduling Status.
* Ease of adding new jobs.
* Ease of adding new functionality.
* Ease of Sending Push Notifications.

# delphi
