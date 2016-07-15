import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.serializer.KryoRegistrator
import java.io.File
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import collection.mutable.ListBuffer
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import com.datastax.spark.connector.mapper.DefaultColumnMapper
import com.datastax.spark.connector.rdd.reader.RowReaderFactory

//must pass cassandra ip
//batch layer of #trendOff that takes data from our cassandra "data lake",
//combines user's tweets and then saves the user and combined tweets to 
//a new cassandra table

//case class for the data in the cassandra "data lake"
case class individTweet(id: Int, user: String,tweettext: String, favorite: Boolean, isretweet: Boolean,
                        retweeted: Boolean, sensitive: Boolean,date: java.util.Date)
object individTweet {//convert each row from the cassandra "data lake" 
//into our case class individTweet
  implicit object Mapper extends DefaultColumnMapper[individTweet](
    Map("id" -> "id", "user" -> "user","tweettext" -> "tweettext","favorite" -> "favorite","isretweet" -> "isretweet",
      "retweeted" -> "retweeted","sensitive" -> "sensitive","date" -> "date"))
}

object SimpleApp{
  def main(args : Array[String]): Unit ={
    val conf = new SparkConf()//set up spark configuration
      .setMaster("local[4]")
      .setAppName("SimpleApp")
      .set("spark.cassandra.connection.host", args(0))
    val sc = new SparkContext(conf)//generate spark context
    val cassandraConnector = CassandraConnector(conf)//connect to cassandra
    //read in data from cassandra and convert each row to an instance of individTweet
    val data = sc.cassandraTable[individTweet]("twitter","streamtable")
    val dropTable = "DROP TABLE IF EXISTS twitter.batchtweettext"
    val createTable = "CREATE TABLE IF NOT EXISTS twitter.batchtweettext(user text PRIMARY KEY,tweettext text);"

    cassandraConnector.withSessionDo { session =>
      session.execute(dropTable)//delete old batch table
      session.execute(createTable)//create the new batch table
    }

    val intermediate = data.map(j => (j.user,j.tweettext)).reduceByKey(_++_).saveToCassandra("twitter", "batchtweettext", SomeColumns("user","tweettext"))
    //combine each user's tweet into a string and save the user and combined string into cassandra
  }
}