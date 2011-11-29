import sbt._
import Keys._
import io.backchat.sbt._
import GlobalSettings._
import Dependencies._
import sbtprotobuf.{ProtobufPlugin=>PB}

object BackchatBorgBuild extends Build {

  val buildOrganization = "com.mojolly.borg"
  val buildVersion      = "0.0.5-SNAPSHOT"
  
  val commonSettings = buildSettings ++ Seq(
    version := buildVersion, 
    organization := buildOrganization)

  val root = (Project ("backchat-borg", file("."), settings = commonSettings ++ Seq(
      mainClass := Some("backchat.borg.samples.ClientTestServer"),
      description := "Provides the clustering and High-Availabillity for the backchat system"))
      aggregate(core, hive, cadence, samples, assimil))

  lazy val core =  (Project("borg-core", file("core"), settings = commonSettings ++ PB.protobufSettings ++ Seq(
    description := "The shared classes for the borg",
    ivyXML := <dependencies>
                <exclude module="slf4j-log4j12" />
                <exclude module="log4j" />
              </dependencies>,
    unmanagedResourceDirectories in Compile <+= (sourceDirectory in PB.protobufConfig).identity,
    javaSource in PB.protobufConfig <<= (sourceManaged in Compile).identity,
    libraryDependencies ++= Seq(
      "com.google.protobuf" % "protobuf-java" % "2.4.1",
      mojollyLibrary("core"),
      mojollyLibrary("io"),
      commons("codec", "1.5"),
      akka("slf4j"),
      specs2, scalaTest, akkaTestkit, mockito, mojollyLibrary("testing") % "test"
    ))))

  lazy val hive = Project("borg-hive", file("hive"), settings = commonSettings ++ PB.protobufSettings ++ Seq(
    libraryDependencies += "org.apache.hadoop.zookeeper" % "zookeeper" % "3.4.0",
    unmanagedResourceDirectories in Compile <+= (sourceDirectory in PB.protobufConfig).identity,
    javaSource in PB.protobufConfig <<= (sourceManaged in Compile).identity,
    description := "Provides the clustering and High-Availabillity for the backchat system",
    ivyXML := <dependencies>
                <exclude module="slf4j-log4j12" />
                <exclude module="log4j" />
              </dependencies>)) dependsOn(core % "compile;test->test")

  lazy val cadence = Project("borg-cadence", file("cadence"), settings = commonSettings ++ Seq(
    description := "Provides the metrics collection for the hive",
    libraryDependencies += mojollyLibrary("metrics")
  )) dependsOn(core % "compile;test->test")

  lazy val samples = (Project("borg-samples", file("samples"), settings = commonSettings ++ Seq(
    description := "Some sample applications written with backchat-borg"
  )) dependsOn(hive, assimil, cadence, core % "compile;test->test"))
  
  lazy val assimil = Project("borg-assimil", file("assimil"), settings = commonSettings ++ Seq(
    description := "Contains the communication devices for the borg"
  )) dependsOn(core % "compile;test->test")
                
}
// vim: set ts=2 sw=2 et:

