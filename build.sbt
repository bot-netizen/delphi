//Project Level Configurations
val sparkVersion = "2.2.0"

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "delphi",
    version := "0.1.0",
    scalaVersion := "2.11.8",
    organization := "ca.ix.delphi",
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion),
    buildInfoPackage := "ca.ix.delphi"
  )

// Testing and coverage Settings.
coverageExcludedPackages := "ca\\.ix\\.delphi\\.runner;"
coverageMinimum := 80
coverageFailOnMinimum := false

// Fork Multiple JVM in test
fork in Test := true
test in assembly := {}

//Assembly configurations
javaOptions ++= Seq("-Xms2G", "-Xmx4G", "-XX:MaxPermSize=4096M", "-XX:+CMSClassUnloadingEnabled")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(appendContentHash = false)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}

// Scala Fmt settings.
scalafmtOnCompile := true
scalafmtTestOnCompile := false

//Project Dependencies
libraryDependencies ++= Seq(
  dependencies.hubbleCommons,
  dependencies.scalaTest,
  dependencies.scalaMockTest,
  dependencies.scalaLivy,
  dependencies.javaLivy
)

// https://mvnrepository.com/artifact/com.google.code.gson/gson
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.5"

// Project Dependencies
lazy val dependencies =
  new {
    val hubbleCommonV = "0.40.0"
    val scalaTestV = "3.0.4"
    val scalaTestMockV = "4.2.0"
    val json4sV = "3.6.7"
    val hubbleCommons = "ca.ix.hubble" %% "hubble-commons" % hubbleCommonV
    // exclude ("org.glassfish.hk2", "hk2-utils") exclude ("org.glassfish.hk2", "hk2-locator") exclude ("javax.validation", "validation-api")
    val scalaTest = "org.scalatest" %% "scalatest" % scalaTestV % "test"
    val scalaMockTest = "org.scalamock" %% "scalamock" % scalaTestMockV % "test"
    val json4s = "org.json4s" %% "json4s-jackson" % json4sV
    val scalaHttp = "org.scalaj" % "scalaj-http_2.11" % "2.3.0"
    val scalaLivy = "org.apache.livy" %% "livy-scala-api" % "0.6.0-incubating"
    val javaLivy = "org.apache.livy" % "livy-api" % "0.6.0-incubating"

  }
