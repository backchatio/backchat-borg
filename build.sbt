import io.backchat.sbt._
import GlobalSettings._
import Dependencies._
import sbtprotobuf.{ProtobufPlugin => PB}

seq(buildSettings:_*)

seq(PB.protobufSettings:_*)

version := "0.0.5-SNAPSHOT"

organization := "com.mojolly.borg"

mainClass := Some("backchat.borg.samples.ClientTestServer")

description := "Provides the clustering and High-Availabillity for the backchat system"

ivyXML := <dependencies>
                <exclude module="slf4j-log4j12" />
                <exclude module="log4j" />
              </dependencies>

unmanagedResourceDirectories in Compile <+= (sourceDirectory in PB.protobufConfig)

javaSource in PB.protobufConfig <<= (sourceManaged in Compile)

libraryDependencies ++= Seq(
  "com.google.protobuf" % "protobuf-java" % "2.4.1",
  mojollyLibrary("core"),
  mojollyLibrary("io"),
  mojollyLibrary("metrics"),
  commons("codec", "1.5"),
  akka("slf4j"),
  "org.apache.hadoop.zookeeper" % "zookeeper" % "3.4.0",
  specs2,
  scalaTest,
  akkaTestkit,
  mockito,
  mojollyLibrary("testing") % "test"
)