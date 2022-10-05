import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object MovieCountry {
  def main(args: Array[String]): Unit = {
    var sparkConf: SparkConf = null
    sparkConf = new SparkConf()
      .set("spark.sql.crossJoin.enabled", "true")

    // Create the spark session first
    val ss = SparkSession.builder().master("local").appName("tfidfApp").getOrCreate()
    import ss.implicits._ // For implicit conversions like converting RDDs to DataFrames

    // Read the contents of the csv file in a dataframe. The csv file contains a header.
    val basicDF = ss.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .option("inferSchema",true)
      .csv("data/exercise4/movies.csv")
    //basicDF.printSchema() //print schema of dataframe

    // Define some helpful User-Defined Functions (UDFs)
    val udf_toDouble = udf[Double, String](_.toDouble)
    val udf_toInt = udf[Int, String](_.toInt)


    // Convert columns to the appropriate data type.
    val modifiedDF = basicDF
      .withColumn("imdbID", udf_toInt($"imdbID"))
      .withColumn("year", udf_toInt($"year"))
      .withColumn("imdbRating", udf_toDouble($"imdbRating"))
      .withColumn("imdbVotes", udf_toInt($"imdbVotes"))


    val modifiedDF2 = basicDF
      .withColumn("imdbRating", col("imdbRating").cast(DoubleType) )
      .withColumn("imdbID", col("imdbID").cast(IntegerType) )
      .filter(basicDF("imdbRating").isNotNull)
      .filter(basicDF("imdbID").isNotNull)
      .na.drop()

    modifiedDF.createOrReplaceTempView("movies")
    modifiedDF.printSchema() //print schema of dataframe

    //Α.
    println("A")
    //Running SQL Queries Programmatically: use the sqlContext to run SQL queries, since "movies" is like a database table.
    //Δεν μπορεί να εκτελεστεί καθόσον εμφανίζει (string) => double) error
    //modifiedDF2.sqlContext.sql("SELECT cast(Year as int) Year, count(cast(Year as int)) as Plithos, AVG(cast(imdbRating as double)) FROM movies group by year").show()

    val DFA = modifiedDF2
      .groupBy($"year")
      .agg(count("imdbRating").as("Πλήθος"), avg("imdbRating").as("imdbRating"))
      .toDF()
      //.write.option("header",true).csv("data/exercise4/output/thema3/3a")
      .show()

    //B
    println("B")
    val DFB =modifiedDF2
      .groupBy($"country")
      .agg(max("imdbRating"),first("title").as("Τίτλος"),first("year")as("Έτος"))
      .orderBy("country")
      .toDF()
      //.write.option("header",true).csv("data/exercise4/output/thema3/3b")
      .show()

    //C
    println("C")
    /*Λύση SQL που όμως χτυπάει error μετατροπής
    modifiedDF2
      .sqlContext.sql("select DISTINCT movies.title, movies.imdbRating, " +
      "movies2.title, movies2.imdbRating " +
      "from movies CROSS JOIN movies as movies2 " +
      "where abs(movies.imdbRating-movies2.imdbRating)>=1 and (movies.imdbID < movies2.imdbID)  ").show()
    */
    val newNames = Seq("imdbID", "title", "year", "runtime","genre","released","imdbRating","imdbVotes","country","imdbID2", "title2", "year2", "runtime2","genre2","released2","imdbRating2","imdbVotes2","country2")
    val DFC = modifiedDF2.crossJoin(modifiedDF2)
      //.filter(column("imdbRating"))
      .toDF(newNames: _*)
      .drop("genre").drop("genre2").drop("released").drop("released2").drop("imdbVotes").drop("imdbVotes2")
      .filter(abs($"imdbRating"-$"imdbRating2")<=1)   //κριτήριο εμφάνισης εργασίας
      .filter($"imdbID"<$"imdbID2") //για να υπάρχουν μία φορά ο συνδιασμός α-β και να μην υπάρχει α-α
      //.write.option("header",true).csv("data/exercise4/output/thema3/3c")
      .show()

    //DFC.printSchema()


  } //end Main
} //end obj

