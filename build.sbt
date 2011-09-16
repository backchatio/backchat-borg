
name := "akka-zeromq"

version := "0.0.2-SNAPSHOT"

organization := "com.mojolly.zeromq"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-optimize", "-unchecked", "-deprecation", "-Xcheckinit", "-encoding", "utf8")

resolvers ++= Seq(
  "GlassFish Repo" at "http://download.java.net/maven/glassfish/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "ScalaTools Snapshots" at "http://scala-tools.org/repo-snapshots",
  "Akka Repo" at "http://akka.io/repository"
)

libraryDependencies ++= Seq(
  "net.liftweb" %% "lift-json" % "2.4-M4",
  "net.liftweb" %% "lift-json-ext" % "2.4-M4",
  "org.scala-tools.time" %% "time" % "0.5",
  "se.scalablesolutions.akka" % "akka-stm" % "1.2-RC6",
  "com.mojolly.logback" %% "logback-akka" % "0.7.2-SNAPSHOT",
  "org.scalatest" %% "scalatest" % "1.6.1" % "test",
  "se.scalablesolutions.akka" % "akka-testkit" % "1.2-RC6" % "test"
)


parallelExecution in Test := false

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishTo <<= (version) { version: String =>
  val nexus = "http://maven.mojolly.com/content/repositories/"
  if (version.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus+"snapshots/")
  else                                   Some("releases" at nexus+"releases/")
}

