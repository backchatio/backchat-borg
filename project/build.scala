import sbt._
import Keys._
import io.backchat.sbt._
import GlobalSettings._
import Dependencies._
import sbtprotobuf.{ProtobufPlugin=>PB}

object BackchatBorgBuild extends Build {

  val buildOrganization = "com.mojolly.borg"
  val buildVersion      = "0.0.4-SNAPSHOT"
  
  val commonSettings = buildSettings ++ Seq(version := buildVersion, organization := buildOrganization)
  val typesafeReleases = "TypeSafe releases" at "https://artifactory.backchat.io/typesafe-releases/"

  lazy val root = Project ("backchat-borg", file("."), settings = commonSettings ++ PB.protobufSettings ++ Seq(
      moduleConfigurations += ModuleConfiguration("org.apache.hadoop.zookeeper", typesafeReleases),
      libraryDependencies ++= Seq(
        "com.google.protobuf" % "protobuf-java" % "2.4.1",
        mojollyLibrary("core"), commons("codec", "1.5"), commons("io", "2.1"),
        "org.slf4j" % "log4j-over-slf4j" % "1.6.1",
        "org.apache.hadoop.zookeeper" % "zookeeper" % "3.4.0",
        akka("stm"), specs2, scalaTest, akkaTestkit, mockito
      ),
      unmanagedResourceDirectories in Compile <+= (sourceDirectory in PB.protobufConfig).identity,
      javaSource in PB.protobufConfig <<= (sourceManaged in Compile).identity,
      ivyXML := <dependencies>
          <exclude module="slf4j-log4j12" />
          <exclude module="log4j" />
        </dependencies>,
      mainClass := Some("backchat.borg.samples.ClientTestServer"),
      description := "Provides the clustering and High-Availabillity for the backchat system"))
  
}
// vim: set ts=2 sw=2 et:

