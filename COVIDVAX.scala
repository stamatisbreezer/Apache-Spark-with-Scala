import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object thema4 {
  def main(args: Array[String]): Unit = {
    var sparkConf: SparkConf = null
    sparkConf = new SparkConf()
      .set("spark.sql.crossJoin.enabled", "true")

    // Create the spark session first
    val ss = SparkSession.builder().master("local").appName("tfidfApp").getOrCreate()
    import ss.implicits._ // For implicit conversions like converting RDDs to DataFrames

    // Read the contents of the csv file in a dataframe. The csv file contains a header.
    val basicDF = ss.read
      .option("header", "true") //αποδεχόμαστε τους τίτλους της πρώτης γραμμής
      .option("mode", "DROPMALFORMED") //απορρίπτουμε τα δεδομένα που δεν είναι σωστά στις στήλες (σκουπίδια)
      .option("inferSchema", true) //αποδεχόμαστε την αυτόματη αναγνώριση των στηλών
      .csv("country_vaccinations_by_manufacturer.csv")
    //basicDF.printSchema() //print schema of dataframe

    // Define some helpful User-Defined Functions (UDFs)
    val udf_toDouble = udf[Double, String](_.toDouble)
    val udf_toInt = udf[Int, String](_.toInt)



    val modifiedDF = basicDF
      .withColumn("total_vaccinations", col("total_vaccinations").cast(IntegerType) )
      .filter(basicDF("date").isNotNull)
      .filter(basicDF("total_vaccinations").isNotNull)
      .na.drop()


    modifiedDF.printSchema() //print schema of dataframe
    //modifiedDF.show()

    //1a
    val modifiedDF1=modifiedDF
      .groupBy($"date",$"location")
      .agg(sum("total_vaccinations") as "suma")

    print("1a The avairage for all the days:  ")
    val modifiedDF1a=modifiedDF1
       .agg(avg("suma") as("Average") )
      .createOrReplaceTempView("average")

    val temp = ss.sql("select Average from average").first()
            .get(0)
    println(temp)


    //1B-C
    println("1b-c")
    val modifiedDF1C=modifiedDF1
      .withColumn("year", split(col("date"), "-").getItem(0))
      .withColumn("month", split(col("date"), "-").getItem(1))
      .withColumn("day", split(col("date"), "-").getItem(2))
      .groupBy(($"year"))
      .agg(avg("suma") as("Average") )
      .orderBy(desc_nulls_last("Average"))
      .show()

    //2
    println("2")
    val modifiedDF2=modifiedDF
      .groupBy($"date",$"location")
      .agg(sum("total_vaccinations") as "suma")
      .orderBy(desc_nulls_last("suma"))
      .filter($"suma">temp)
      .toDF()
      //.write.option("header",true).csv("data/exercise4/output/thema4/2")
      .show()

    //3
    println("3")
    val modifiedDF3a=modifiedDF
      .groupBy($"location",$"vaccine")
      .agg(sum("total_vaccinations") as "vaccinations")
      .orderBy(asc_nulls_last("location"))


    val modifiedDF3min=modifiedDF3a
      .groupBy("location")
      .agg(min("vaccinations") as "least")
      .orderBy(asc_nulls_last("location"))
      .toDF()
      //.write.option("header",true).csv("data/exercise4/output/thema4/3min")
      .show()

    val modifiedDF3max=modifiedDF3a
      .groupBy("location")
      .agg(max("vaccinations") as "most")
      .orderBy(asc_nulls_last("location"))
      .toDF()
      //.write.option("header",true).csv("data/exercise4/output/thema4/3max")
      .show()



  }


}
