// properties

val APP_VERSION = "0.5.0-SNAPSHOT"
val SCALA_VERSION = "2.11.8"
val SPARK_VERSION = "1.6.1"

crossScalaVersions := Seq("2.10.5", SCALA_VERSION)

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
resolvers += "Bintray megamsys" at "https://dl.bintray.com/megamsys/scala/"

// run options

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

// test options

fork in Test := true
javaOptions in Test += "-XX:MaxMetaspaceSize=512m"
parallelExecution in Test := false
testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")
ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := false

// eclipse options

unmanagedSourceDirectories in Compile <<= (scalaSource in Compile)(Seq(_))
unmanagedSourceDirectories in Test <<= (scalaSource in Test)(Seq(_))
EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource
EclipseKeys.withSource := true
EclipseKeys.classpathTransformerFactories := Seq(AddSourcesTransformer)

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
  "org.apache.spark"         %% "spark-mllib"                % SPARK_VERSION    % "provided",
  "org.apache.spark"         %% "spark-hive"                 % SPARK_VERSION    % "provided"
)

val testLibs = Seq(
  "org.specs2"               %% "specs2"                     % "3.7"            % "test",
  "org.scalacheck"           %% "scalacheck"                 % "1.12.5"         % "test",
  "de.flapdoodle.embed"       % "de.flapdoodle.embed.mongo"  % "1.50.5"         % "test",
  "com.novocode"              % "junit-interface"            % "0.11"           % "test",
  "com.h2database"            % "h2"                         % "1.4.192"        % "test"
)

val cassandraLibs = Seq(
  "com.datastax.spark"       %% "spark-cassandra-connector"  % "1.6.0"
)

def newmanLib(version: String) = version match {
  case "2.10.5" => "com.stackmob" %% "newman" % "1.3.5" exclude("com.typesafe.akka", "akka-actor_2.10")
  case SCALA_VERSION => "io.megam" %% "newman" % "1.3.12" exclude("com.typesafe.akka", "akka-actor_2.11")
}

libraryDependencies ++= Seq(
  "org.mvel"                  % "mvel2"                      % "2.0",
  "org.json4s"               %% "json4s-jackson"             % "3.2.11",
  "io.gatling"               %% "jsonpath"                   % "0.6.7",
  "com.github.scopt"         %% "scopt"                      % "3.5.0",
  "org.apache.commons"        % "commons-math3"              % "3.6.1",
  "org.mongodb"              %% "casbah"                     % "3.1.1"
) ++ sparkLibs ++ cassandraLibs ++ testLibs 

libraryDependencies <+= scalaVersion(newmanLib(_))
