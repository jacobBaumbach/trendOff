name := "SimpleApp"

version := "1.0"

scalaVersion := "2.10.4"

resolvers:=Seq("Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "releases" at "http://oss.sonatype.org/service/local/staging/deploy/maven2/")

// https://mvnrepository.com/artifact/org.nd4j/nd4j-x86
libraryDependencies ++= Seq("com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.6.0",
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark"%%"spark-streaming"%"1.6.1",
  "org.apache.spark" %% "spark-sql" % "1.6.1",
  "com.datastax.cassandra"%"cassandra-driver-core"%"3.0.0",
  "org.apache.commons" % "commons-lang3" % "3.3.2",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.1",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.1",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.1",
  "com.typesafe" % "config" % "1.3.0",
  "org.apache.kafka" % "kafka_2.10" % "0.8.0",
  "com.twitter"%%"bijection-avro"%"0.9.2",
  "org.apache.avro" % "avro" % "1.8.1",
  //"com.sksamuel.avro4s" % "avro4s-core_2.10" % "1.2.2",
  //"com.twitter"%"bijection-core_2.11"%"0.9.2",
  "org.apache.commons" % "commons-parent" % "33" pomOnly())

// https://mvnrepository.com/artifact/org.apache.avro/avro// https://mvnrepository.com/artifact/com.sksamuel.avro4s/avro4s-macros_2.12.0-M3
//libraryDependencies +=




//"org.apache.commons" % "commons-lang3" % "3.3.2"
// https://mvnrepository.com/artifact/org.apache.commons/commons-parent
// https://mvnrepository.com/artifact/org.apache.kafka/kafka_2.10










