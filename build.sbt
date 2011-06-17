
name := "akka-zeromq"

version := "0.0.1-SNAPSHOT"

organization := "com.mojolly.zeromq"

scalaVersion := "2.9.0-1"

scalacOptions ++= Seq("-optimize", "-unchecked", "-deprecation", "-Xcheckinit", "-encoding", "utf8")

resolvers ++= Seq(
  "GlassFish Repo" at "http://download.java.net/maven/glassfish/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "ScalaTools Snapshots" at "http://scala-tools.org/repo-snapshots",
  "Akka Repo" at "http://akka.io/repository"
)

libraryDependencies ++= Seq(
  "net.liftweb" %% "lift-json" % "2.4-M1",
  "org.scala-tools.time" %% "time" % "0.4",
  "se.scalablesolutions.akka" % "akka-stm" % "1.1.2",
  "com.weiglewilczek.slf4s" %% "slf4s" % "1.0.6",
  "ch.qos.logback" % "logback-classic" % "0.9.28",
  "org.scalatest" % "scalatest_2.9.0" % "1.6.1" % "test",
  "se.scalablesolutions.akka" % "akka-testkit" % "1.1.2" % "test"
)


parallelExecution in Test := false

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishTo <<= (version) { version: String =>
  val nexus = "http://maven.mojolly.com/content/repositories/"
  if (version.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus+"snapshots/")
  else                                   Some("releases" at nexus+"releases/")
}

