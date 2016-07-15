import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.serializer.KryoRegistrator
import java.io.File
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import twitter4j.TwitterFactory
import twitter4j.Twitter
import twitter4j. FilterQuery
import twitter4j.conf.ConfigurationBuilder
import collection.mutable.ListBuffer
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import org.apache.spark.sql.SQLContext
//must pass cassandra ip, location of csv file
//takes some data from tweet data from csv and adds it to 
//the cassandra data lake
object SimpleApp{
  def main(args : Array[String]): Unit ={
    //generate spark conference with Cassandra Connector
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("SimpleApp")
      .set("spark.cassandra.connection.host", args(0))

    val sc = new SparkContext(conf)//generate Spark context
    val sqlContext = new SQLContext(sc)//generate Spark SQL context
    val columnNames = Seq("Tweet Id","Date","Nickname","Tweet content")//columns of csv file
    //load csv data into dataframe
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").option("mode", "DROPMALFORMED").load(args(1))
    //convert dataframe into rdd
    val rdd = df.select(columnNames.head, columnNames.tail: _*).rdd//filter(df("Tweet language (ISO 639-1)")="en")

    //https://github.com/databricks/reference-apps/blob/master/twitter_classifier/scala/src/main/scala/com/databricks/apps/twitter_classifier/Utils.scala
    val createKeyspace = "CREATE KEYSPACE IF NOT EXISTS twitter WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };"
    val createTable = "CREATE TABLE IF NOT EXISTS twitter.streamtable(id bigint PRIMARY KEY, user text,tweettext text, favorite boolean, isretweet boolean, retweeted boolean, sensitive boolean,date timestamp);"
    
    val cassandraConnector = CassandraConnector(conf)
    cassandraConnector.withSessionDo { session =>
      session.execute(createKeyspace)//generate keyspace if it does not exist
      session.execute(createTable)//generate table if does not exist
    }
    //put csv data(which is in rdd format) into cassandra table
    rdd.filter{case Row(tw: String, date: String,userName: String, tweettext: String) => org.apache.commons.lang.math.NumberUtils.isNumber(tw.trim)}.map{case Row(tw: String, date: String,userName: String, tweettext: String) =>
            (tw.trim.toLong,userName,tweettext,false,false,false,false,org.apache.commons.lang3.time.DateUtils.addDays(new java.util.Date(),-365*3))}.saveToCassandra("twitter", "streamtable", SomeColumns("id", "user","tweettext","favorite","isretweet","retweeted","sensitive",
      "date"))

  }
}