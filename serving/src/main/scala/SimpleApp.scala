import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.serializer.KryoRegistrator
import java.io.File
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import collection.mutable.ListBuffer
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import org.apache.spark.sql.SQLContext
import com.datastax.spark.connector.mapper.DefaultColumnMapper
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import java.io._
import scala.collection.Map
//must pass cassandra ip, username1, username2, location to save file
//serving layer that compares word counts for two given users

case class SpeedTweet(user:String,tweettext:String,date:java.util.Date)//case class for speed table

object SpeedTweet{//convert each row of speed table into instance of case class SpeedTweet
  implicit object Mapper extends DefaultColumnMapper[SpeedTweet](
    scala.collection.immutable.Map("user"->"user","tweettext"->"tweettext","date"->"date")
  )
}

case class BatchTweet(user:String,tweettext:String)//case class for batch table

object BatchTweet{//convert each row of batch table into instance of case class BatchTweet
  implicit object Mapper extends DefaultColumnMapper[BatchTweet](
    scala.collection.immutable.Map("user"->"user","tweettext"->"tweettext")
  )
}

object SimpleApp{
  def main(args: Array[String]): Unit ={
    val name1 = args(1)//first user name to be used for comparison
    val name2 = args(2)//second user name to be used for comparison

    val conf = new SparkConf()//set up spark configuration and connect to cassandra
      .setMaster("local[4]")
      .setAppName("SimpleApp")
      .set("spark.cassandra.connection.host",args(0))

    val sc = new SparkContext(conf)//generate sparkcontext
    val cassandraConnector = CassandraConnector(conf)//generate cassandra connector

    def getMap(name:String):Map[String,Int]= {//return a word count map for given user


      def getBatchData() = {//grab batch data for user name and return all tweets string
        val table = sc.cassandraTable[BatchTweet]("twitter", "batchtweettext").select("user", "tweettext")
        table.filter(_.user==name).collect()(0).tweettext
      }

      def getSpeedData() = {//grab speed data for user name and return most recently generated string
        val intermedSpeed = sc.cassandraTable[SpeedTweet]("twitter", "speedtable").select("user", "tweettext", "date")
        intermedSpeed.filter(_.user==name).map(i=>(i.date,(i.tweettext,i.user))).sortByKey(false).take(1)(0)._2._1
      }

      def getMapInner(batchString:String,speedString:String)={//get batch and speed text
        //combine the text, perform word count on the string and convert to a map word -> int
        val completeString = batchString++speedString
        sc.parallelize(completeString.split(" ")).map(i => (i,1)).reduceByKey(_+_).collectAsMap

      }

      getMapInner(getBatchData(),getSpeedData())
    }

    def getBars(name1Map:Map[String,Int],name2Map:Map[String,Int])={//return at most top 100 mutually used words
      // along with the number of times each word was used by the two users 
      //(Array of words, Array of name1 counts, Array of name2 counts)

      def getTopKeys()={//return top words used for each user that both users have used with no repeats
        def getMutualKeys(nameMap:Map[String,Int])={//grab words for user that both users have used
          nameMap.keysIterator.filter(i => name1Map.contains(i) && name2Map.contains(i)).toList
        }

        @tailrec
        def getTopKeysInner(name1KeysI:List[String],name2KeysI:List[String],allWords:scala.collection.mutable.ListBuffer[String],
                            wordTracker:scala.collection.mutable.Set[String], cntr : Int,totalWords : Int) :scala.collection.mutable.ListBuffer[String] ={
          //recursion look for top words used for each user that both users have used with no repeats
          if(totalWords>=100 || name1KeysI.length==0 || name2KeysI.length == 0){
            allWords
          }
          else if(cntr%2==0){
            if(wordTracker.contains(name2KeysI.head)){
              getTopKeysInner(name1KeysI,name2KeysI.tail,allWords+="",wordTracker,cntr+1,totalWords)
            }
            else{
              getTopKeysInner(name1KeysI,name2KeysI.tail,allWords+=name2KeysI.head,wordTracker+=name2KeysI.head,cntr+1,totalWords+1)
            }
          }
          else{
            if(wordTracker.contains(name1KeysI.head)){
              getTopKeysInner(name1KeysI.tail,name2KeysI,allWords+="",wordTracker,cntr+1,totalWords)
            }
            else{
              getTopKeysInner(name1KeysI.tail,name2KeysI,allWords+=name1KeysI.head,wordTracker+=name1KeysI.head,cntr+1,totalWords+1)
            }
          }

        }
        getTopKeysInner(getMutualKeys(name1Map),getMutualKeys(name2Map),new scala.collection.mutable.ListBuffer[String](),
          scala.collection.mutable.Set[String](),0,0)
      }

      def getValues(keys:scala.collection.mutable.ListBuffer[String])={//get number of times each user used the 
        //top words
        def getValuesInner(nameMap:Map[String,Int])={//get number of times given user used the top word
          keys.filterNot(_=="").flatMap(i => nameMap get i)
        }
        (keys,getValuesInner(name1Map),getValuesInner(name2Map))
      }

      getValues(getTopKeys())

    }


    val bars= getBars(getMap(name1),getMap(name2))//get Array of top words, 
    //Array of counts of time name1 used the words, Array of counts of time name2 used the words
    val barsForPrint = bars.zipped.map((i,j,k) => (i,j,k))//convert Arrays to readable format when saved
    sc.parallelize(barsForPrint).coalesce(1).saveAsTextFile(args(3))//save words and counts to file specified by user


  }
}
