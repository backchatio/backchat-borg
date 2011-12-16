resolvers += "gseitz@github" at "http://gseitz.github.com/maven/"

resolvers += Resolver.url("BackChat.IO plugin snapshots", url("https://artifactory.backchat.io/plugins-snapshot-local"))(Resolver.defaultIvyPatterns)

addSbtPlugin("com.mojolly.sbt" % "backchat-sbt" % "0.0.42-SNAPSHOT")

addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.2")
