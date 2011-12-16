import io.backchat.sbt._
import GlobalSettings._
import Dependencies._
import sbtprotobuf.{ProtobufPlugin => PB}

seq(buildSettings:_*)

seq(formatSettings:_*)

seq(PB.protobufSettings:_*)

name := "backchat-borg"

version := "0.1.5-SNAPSHOT"

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
  "org.zeromq" %% "zeromq-scala-binding" % "0.0.3",
  commons("codec", "1.5"),
  akka("slf4j"),
  "org.apache.hadoop.zookeeper" % "zookeeper" % "3.4.0",
  specs2,
  scalaTest,
  akkaTestkit,
  mockito,
  mojollyLibrary("testing") % "test"
)
