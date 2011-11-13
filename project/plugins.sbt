resolvers += "gseitz@github" at "http://gseitz.github.com/maven/"

libraryDependencies += "org.scalariform" %% "scalariform" % "0.1.1"

resolvers += "BackChat.IO plugin releases" at "https://artifactory.backchat.io/plugins-release-local"

resolvers += "BackChat.IO plugin snapshots" at "https://artifactory.backchat.io/plugins-snapshot-local"

addSbtPlugin("com.mojolly.sbt" % "backchat-sbt" % "0.0.5-SNAPSHOT")

addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.2")
