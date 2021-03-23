//for creating testing report and coverage data
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

//for creating fat jar
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")

//for scala linting
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

//for git commands to run in sbt
//addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.9.3")

// Build Info Version in spark job.
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.7.0")

//Add Scala Formatting
addSbtPlugin("com.lucidchart" % "sbt-scalafmt" % "1.15")

// ScapeGoat
addSbtPlugin("com.sksamuel.scapegoat" %% "sbt-scapegoat" % "1.0.9")
