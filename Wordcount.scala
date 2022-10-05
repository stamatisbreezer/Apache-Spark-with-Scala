import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.SparkSession

object Wordcount {

    def setFileName(name: String): Unit = {

      // Create a basic SparkSession, SparkSession.builder():
      val ss1 = SparkSession.builder().master("local")
        .config("spark.some.config.option", "some-value")
        .appName("WordCount")
        .getOrCreate()

      // Read a file containing points and create an RDD
      val wordsRDD = ss1.sparkContext.textFile("data/exercise4/"+name, 2)
        // replacement of all punctuation marks except apostrophe with ""
        .map(_.replaceAll("[,.!?:;=)(#\"\\[\\]]", ""))
        //remove line changes - αφαίρεση αλλαγών γραμμής
        .map(_.replaceAll("\\r+\\n+|\\r+|\\n+", " "))
        // turn all characters to lower case
        .map(_.toLowerCase)
        .flatMap( _.split("""[\s,.;:!?]+""") )
        .sliding(2)
        .map{ case Array(x, y) => ((x , y),1) }
        .reduceByKey( _ + _ )
        .sortBy( z => (z._2, z._1._1, z._1._2), ascending = false )

      //wordsRDD.take(10).foreach(println) //viewing lines for debuging

      val wordCount = wordsRDD.count();
      printf("Total Pairs %s: %d \n",name,wordCount);

      def GreaterThan (words: ((String, String), Int), number : Int): Boolean = {
        if (words._1._1.length > number && words._1._2.length > number) { //apply lenght parameter - εφαρμόζω τις συνθήκη μήκους
          true
        } else false
      }

      val wordLen = wordsRDD
        .filter(GreaterThan(_,3))
        .count()
      printf("Total pairs %s with words >3: %d \n",name,wordLen);

      val mostCommon = wordsRDD
        // filter the records by length
        .filter(GreaterThan(_,3))
        // count the appearances of each word
        .reduceByKey(_ + _)
        // sort by the value descending
        .sortBy(-_._2)
        // keep in the result only the first 5 words
        .take(5)
        .foreach(println)
    }

    def main(args: Array[String]): Unit = {
      setFileName("SherlockHolmes.txt")
      setFileName("Shakespeare.txt")
    } //main end

  } //object end