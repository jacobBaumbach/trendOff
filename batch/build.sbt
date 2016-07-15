name := "SimpleApp"

version := "1.0"

scalaVersion := "2.10.4"

resolvers:=Seq("Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "releases" at "http://oss.sonatype.org/service/local/staging/deploy/maven2/")

// https://mvnrepository.com/artifact/org.nd4j/nd4j-x86
libraryDependencies ++= Seq("org.scalaz" %% "scalaz-core" % "7.2.4",
  "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.6.0",
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark"%%"spark-streaming"%"1.6.1",
  "org.apache.spark" %% "spark-sql" % "1.6.1",
  "com.datastax.cassandra"%"cassandra-driver-core"%"3.0.0",
  "org.apache.commons" % "commons-lang3" % "3.3.2",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.1",
  //"com.twitter"%"bijection-core_2.11"%"0.9.2",
  "org.apache.commons" % "commons-parent" % "33" pomOnly())
//"org.apache.commons" % "commons-lang3" % "3.3.2"
// https://mvnrepository.com/artifact/org.apache.commons/commons-parent








