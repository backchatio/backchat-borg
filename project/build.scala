import sbt._
import Keys._
import com.typesafe.sbtscalariform._
import ScalariformPlugin._

// Shell prompt which show the current project, git branch and build version
// git magic from Daniel Sobral, adapted by Ivan Porto Carrero to also work with git flow branches
object ShellPrompt {
 
  object devnull extends ProcessLogger {
    def info (s: => String) {}
    def error (s: => String) { }
    def buffer[T] (f: => T): T = f
  }
  
  val current = """\*\s+([^\s]+)""".r
  
  def gitBranches = ("git branch --no-color" lines_! devnull mkString)
  
  val buildShellPrompt = { 
    (state: State) => {
      val currBranch = current findFirstMatchIn gitBranches map (_ group(1)) getOrElse "-"
      val currProject = Project.extract (state).currentProject.id
      "%s:%s:%s> ".format (currBranch, currProject, LogbackAkkaSettings.buildVersion)
    }
  }
 
}

object LogbackAkkaSettings {
  val buildOrganization = "com.mojolly.borg"
  val buildScalaVersion = "2.9.1"
  val buildVersion      = "0.0.4-SNAPSHOT"

  lazy val formatSettings = ScalariformPlugin.settings ++ Seq(
    formatPreferences in Compile := formattingPreferences,
    formatPreferences in Test    := formattingPreferences
  )

  def formattingPreferences = {
    import scalariform.formatter.preferences._
    (FormattingPreferences()
        setPreference(IndentSpaces, 2)
        setPreference(AlignParameters, true)
        setPreference(AlignSingleLineCaseStatements, true)
        setPreference(DoubleIndentClassDeclaration, true)
        setPreference(RewriteArrowSymbols, true)
        setPreference(PreserveSpaceBeforeArguments, true))
  }

  val description = SettingKey[String]("description")

  val compilerPlugins = Seq(
    compilerPlugin("org.scala-lang.plugins" % "continuations" % buildScalaVersion),
    compilerPlugin("org.scala-tools.sxr" % "sxr_2.9.0" % "0.2.7")
  )

  val buildSettings = Defaults.defaultSettings ++ formatSettings ++ Seq(
      name := "backchat-borg",
      version := buildVersion,
      organization := buildOrganization,
      scalaVersion := buildScalaVersion,
      javacOptions ++= Seq("-Xlint:unchecked"),
      testOptions in Test += Tests.Setup( () => System.setProperty("akka.mode", "test") ),
      scalacOptions ++= Seq(
        "-optimize",
        "-deprecation",
        "-unchecked",
        "-Xcheckinit",
        "-encoding", "utf8",
        "-P:continuations:enable"),
      resolvers ++= Seq(
        "Mojolly Default" at "http://maven.mojolly.com/content/groups/default-repos/",
        "GlassFish Repo" at "http://download.java.net/maven/glassfish/",
        "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
        "TypeSafe releases" at "http://repo.typesafe.com/typesafe/releases/",
        "Akka Repo" at "http://akka.io/repository"
      ),
      //retrieveManaged := true,
      (excludeFilter in formatSources) <<= (excludeFilter) (_ || "*Spec.scala"),
      libraryDependencies ++= Seq(
        "com.mojolly.library" %% "library-core" % "0.9.7-SNAPSHOT",
        "commons-codec" % "commons-codec" % "1.5",
        "net.liftweb" %% "lift-json" % "2.4-M4",
        "net.liftweb" %% "lift-json-ext" % "2.4-M4",
        "org.scala-tools.time" %% "time" % "0.5",
        "zkclient" % "zkclient" % "0.3",
        "org.apache.hadoop.zookeeper" % "zookeeper" % "3.4.0",
        "org.apache.hadoop.zookeeper" % "zookeeper-recipes-lock" % "3.4.0",
        "se.scalablesolutions.akka" % "akka-stm" % "1.2",
        "com.mojolly.logback" %% "logback-akka" % "0.7.3-SNAPSHOT",
        "org.specs2" %% "specs2" % "1.6.1" % "test",
        "org.scalatest" %% "scalatest" % "1.6.1" % "test",
        "se.scalablesolutions.akka" % "akka-testkit" % "1.2" % "test"
      ),
      libraryDependencies ++= compilerPlugins,
      autoCompilerPlugins := true,
      parallelExecution in Test := false,
      publishTo <<= (version) { vers: String => 
        val nexus = "http://maven.mojolly.com/content/repositories/"
        if (vers.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus+"snapshots/")
        else                                Some("releases" at nexus+"releases/")
      },
      mainClass := Some("backchat.borg.samples.ClientTestServer"),
      shellPrompt  := ShellPrompt.buildShellPrompt)

  val packageSettings = Seq (
    packageOptions <<= (packageOptions, name, version, organization) map {
      (opts, title, version, vendor) =>
         opts :+ Package.ManifestAttributes(
          "Created-By" -> System.getProperty("user.name"),
          "Built-By" -> "Simple Build Tool",
          "Build-Jdk" -> System.getProperty("java.version"),
          "Specification-Title" -> title,
          "Specification-Vendor" -> "Mojolly Ltd.",
          "Specification-Version" -> version,
          "Implementation-Title" -> title,
          "Implementation-Version" -> version,
          "Implementation-Vendor-Id" -> vendor,
          "Implementation-Vendor" -> "Mojolly Ltd.",
          "Implementation-Url" -> "https://backchat.io"
         )
    },
    pomExtra <<= (pomExtra, name, description) { (extra, title, desc) => extra ++ Seq(
      <name>{title}</name>, <description>{desc}</description>)
    })
 
  val projectSettings = buildSettings ++ packageSettings
}

object LogbackAkkaBuild extends Build {

  import LogbackAkkaSettings._
  val buildShellPrompt =  ShellPrompt.buildShellPrompt

  lazy val root = Project ("backchat-borg", file("."), settings = projectSettings ++ Seq(
    description := "Provides the clustering and High-Availabillity for the backchat system")) 


  
}
// vim: set ts=2 sw=2 et:

