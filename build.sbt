ThisBuild / resolvers ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
)

name := "Flink Project"

version := "0.1-SNAPSHOT"

organization := "org.example"

ThisBuild / scalaVersion := "2.12.8"

val flinkVersion = "1.7.0"

resolvers += "Confluent" at "https://packages.confluent.io/maven/"

lazy val root = (project in file(".")).
    settings(
        libraryDependencies ++= Seq(
            "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
            "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
            "org.apache.flink" %% "flink-table" % flinkVersion % "provided",
            "org.apache.flink" % "flink-connector-kafka_2.11" % flinkVersion,
            "org.apache.flink" % "flink-avro" % flinkVersion,
            "org.apache.flink" % "flink-avro-confluent-registry" % flinkVersion,
            "io.confluent" % "kafka-schema-registry-client" % "5.3.2"
        )
    )

assembly / mainClass := Some("org.example.Job")

Compile / run := Defaults.runTask(
    Compile / fullClasspath,
    Compile / run / mainClass,
    Compile / run / runner
).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true
// fork := true

// exclude Scala library from assembly
assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)
