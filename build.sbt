
name := "backchat-borg"

version := "0.0.3-SNAPSHOT"

organization := "com.mojolly.borg"

scalaVersion := "2.9.1"

scalacOptions ++= Seq("-optimize", "-unchecked", "-deprecation", "-Xcheckinit", "-encoding", "utf8")

ivyXML := <dependencies>
      <exclude module="jms"/>
      <exclude module="jmxtools"/>
      <exclude module="jmxri"/>
    </dependencies>


resolvers ++= Seq(
  "GlassFish Repo" at "http://download.java.net/maven/glassfish/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "ScalaTools Snapshots" at "http://scala-tools.org/repo-snapshots",
  "Akka Repo" at "http://akka.io/repository"
)

libraryDependencies ++= Seq(
  "commons-codec" % "commons-codec" % "1.5",
  "net.liftweb" %% "lift-json" % "2.4-M4",
  "net.liftweb" %% "lift-json-ext" % "2.4-M4",
  "org.scala-tools.time" %% "time" % "0.5",
  "org.apache.zookeeper" % "zookeeper" % "3.3.0",
  "se.scalablesolutions.akka" % "akka-stm" % "1.2",
  "com.mojolly.logback" %% "logback-akka" % "0.7.3-SNAPSHOT",
  "org.scala-tools.testing" %% "specs" % "1.6.9" % "test",
  "org.scalatest" %% "scalatest" % "1.6.1" % "test",
  "se.scalablesolutions.akka" % "akka-testkit" % "1.2" % "test"
)


parallelExecution in Test := false

publishTo <<= (version) { version: String =>
  val nexus = "http://maven.mojolly.com/content/repositories/"
  if (version.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus+"snapshots/")
  else                                   Some("releases" at nexus+"releases/")
}

mainClass := Some("backchat.borg.samples.ReliableActorServer")