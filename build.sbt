// properties

val APP_VERSION = "0.3.0-SNAPSHOT"
val SCALA_VERSION = "2.10.5"
val SPARK_VERSION = "1.5.1"

// tasks

val gitHeadCommitSha = taskKey[String]("Determines the current git commit SHA")
gitHeadCommitSha := Process("git rev-parse HEAD").lines.headOption.getOrElse("Unknown")
val makeVersionProperties = taskKey[Seq[File]]("Makes a version.properties file")
makeVersionProperties := {
	val propFile = new File((resourceManaged in Compile).value, "version.properties")
	val content = "version=%s" format (gitHeadCommitSha.value)
	IO.write(propFile, content)
	Seq(propFile)
}
resourceGenerators in Compile += makeVersionProperties.taskValue

// settings

name := "ignition"
organization := "com.uralian"
version := APP_VERSION
scalaVersion := SCALA_VERSION
scalacOptions ++= Seq("-unchecked", "-deprecation", "-Xlint", "-Ywarn-dead-code", "-language:_", "-target:jvm-1.7", "-encoding", "UTF-8")
resolvers += "typesafe repo" at "http://repo.typesafe.com/typesafe/releases/"

// run options

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

// test options

parallelExecution in Test := false
testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")
ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := false

// eclipse options

unmanagedSourceDirectories in Compile <<= (scalaSource in Compile)(Seq(_))
unmanagedSourceDirectories in Test <<= (scalaSource in Test)(Seq(_))
EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

// package options

enablePlugins(JavaAppPackaging) 

// assembly options

assemblyJarName in assembly := s"${name.value}-all-${version.value}.jar"
test in assembly := {}
mergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

// publishing options

publishMavenStyle := true
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}
pomIncludeRepository := { _ => false }
pomExtra := (
  <url>http://uralian.com/ignition</url>
  <licenses>
    <license>
      <name>MIT</name>
      <url>http://opensource.org/licenses/MIT</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>https://github.com/uralian/ignition.git</url>
    <connection>scm:https://github.com/uralian/ignition.git</connection>
  </scm>
  <developers>
    <developer>
      <id>uralian</id>
      <name>Vlad Orzhekhovskiy</name>
      <url>http://uralian.com</url>
    </developer>
  </developers>)
  
// BuildInfo options
enablePlugins(BuildInfoPlugin)
buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)
buildInfoPackage := "com.ignition"
    
// dependencies
  
val sparkLibs = Seq(
  "org.apache.spark"         %% "spark-core"                 % SPARK_VERSION    % "provided",
  "org.apache.spark"         %% "spark-streaming"            % SPARK_VERSION    % "provided",
  "org.apache.spark"         %% "spark-streaming-kafka"      % SPARK_VERSION    % "provided",
  "org.apache.spark"         %% "spark-sql"                  % SPARK_VERSION    % "provided",
  "org.apache.spark"         %% "spark-mllib"                % SPARK_VERSION    % "provided"
)

val testLibs = Seq(
  "org.specs2"               %% "specs2"                     % "2.3.12"         % "test",
  "org.scalacheck"           %% "scalacheck"                 % "1.11.3"         % "test",
  "de.flapdoodle.embed"       % "de.flapdoodle.embed.mongo"  % "1.47.2"         % "test",
  "com.github.athieriot"     %% "specs2-embedmongo"          % "0.7.0"          % "test",
  "com.novocode"              % "junit-interface"            % "0.11"           % "test"
)

val cassandraLibs = Seq(
  "com.datastax.spark"       %% "spark-cassandra-connector"  % "1.5.0-M2"
)

libraryDependencies ++= Seq(
  "com.squants"              %% "squants"                    % "0.4.2",
  "com.stackmob"             %% "newman"                     % "1.3.5"
		exclude("com.typesafe.akka", "akka-actor_2.10"),
  "org.mvel"                  % "mvel2"                      % "2.0",
  "org.json4s"               %% "json4s-jackson"             % "3.2.11",
  "io.gatling"               %% "jsonpath"                   % "0.6.2",
  "com.github.scopt"         %% "scopt"                      % "3.3.0",
  "org.apache.commons"        % "commons-math3"              % "3.5",
  "org.mongodb"              %% "casbah"                     % "2.8.0",
  "com.uralian"              %% "sdk-dslink-scala-spark"     % "0.1.0-SNAPSHOT"
  		exclude("org.iot-dsa", "logging")
) ++ sparkLibs ++ cassandraLibs ++ testLibs