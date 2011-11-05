import sbt._
import Keys._
import com.typesafe.sbtscalariform._
import ScalariformPlugin._
import sbtprotobuf.{ProtobufPlugin=>PB}

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
      "%s:%s:%s> ".format (currBranch, currProject, BackchatBorgSettings.buildVersion)
    }
  }
 
}

object BackchatBorgSettings {
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

  val buildSettings = Defaults.defaultSettings ++ PB.protobufSettings ++ formatSettings ++ Seq(
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
        "Mojolly Default" at "https://artifactory.backchat.io/default-repos/",
        "GlassFish Repo" at "https://artifactory.backchat.io/glassfish-repo/",
        "Sonatype Snapshots" at "https://artifactory.backchat.io/sonatype-oss-snapshots/",
        "TypeSafe releases" at "https://artifactory.backchat.io/typesafe-releases/",
        "Akka Repo" at "https://artifactory.backchat.io/akka-repo/"
      ),
      //retrieveManaged := true,
      (excludeFilter in formatSources) <<= (excludeFilter) (_ || "*Spec.scala"),
      libraryDependencies ++= Seq(
        "com.google.protobuf" % "protobuf-java" % "2.4.1",
        "com.mojolly.library" %% "library-core" % "0.9.7-SNAPSHOT",
        "commons-codec" % "commons-codec" % "1.5",
        "net.liftweb" %% "lift-json-ext" % "2.4-M4",
        "org.slf4j" % "log4j-over-slf4j" % "1.6.1",
        "org.apache.hadoop.zookeeper" % "zookeeper" % "3.4.0",
        "se.scalablesolutions.akka" % "akka-stm" % "1.2",
        "com.mojolly.logback" %% "logback-akka" % "0.7.4-SNAPSHOT",
        "org.specs2" %% "specs2" % "1.6.1" % "test",
        "org.scalatest" %% "scalatest" % "1.6.1" % "test",
        "se.scalablesolutions.akka" % "akka-testkit" % "1.2" % "test",
        "org.mockito" % "mockito-all" % "1.8.5" % "test"
      ),
      compileOrder := CompileOrder.JavaThenScala,
      libraryDependencies ++= compilerPlugins,
      autoCompilerPlugins := true,
      parallelExecution in Test := false,
      unmanagedResourceDirectories in Compile <+= (sourceDirectory in PB.protobufConfig).identity,
      javaSource in PB.protobufConfig <<= (sourceManaged in Compile).identity,
      ivyXML := <dependencies>
          <exclude module="slf4j-log4j12" />
          <exclude module="log4j" />
        </dependencies>,
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
    })
 
  val projectSettings = buildSettings ++ packageSettings
}

object BackchatBorgBuild extends Build {

  import BackchatBorgSettings._
  val buildShellPrompt =  ShellPrompt.buildShellPrompt

  lazy val root = Project ("backchat-borg", file("."), settings = projectSettings ++ Seq(
    description := "Provides the clustering and High-Availabillity for the backchat system")) 


  
}
// vim: set ts=2 sw=2 et:

