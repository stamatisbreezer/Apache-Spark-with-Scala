import org.apache.spark.sql.SparkSession

object Marketbasket {

  def main(args: Array[String]): Unit = {

    val plithos = 2; //Less show combination parameter  Παράμετρος ελάχιστης εμφάνισης συνδιασμών

    // Create the spark session first
    val ss = SparkSession.builder().master("local").appName("WordCount").getOrCreate() // For implicit conversions like converting RDDs to DataFrames
    // Read a file containing the groceries and create an RDD
    val psonia = ss.sparkContext.textFile("data/exercise4/groceries.csv")

    def metrisi_Seq (Arr: Array[String]): Seq[String]= {
      var Et = new Tuple2("", 1)
      for { //For each value of table need to pair with rest - Για κάθε στοιχείο του πίνακα θα πρέπει να συνδυαστεί με τα υπόλοιπα
        n <- 0 to (Arr.length - 1) //first will pair - τα πρώτα θα συνδιαστούν
        c <- 1 + n to (Arr.length - 1) //with next - με τα επόμενα
      } yield (Arr(n) + "+" + Arr(c))
    }

    val Dipla3=psonia
      .map(line => {
        var columns = line.split(",").sorted
        metrisi_Seq(columns)
      })


    val Dipla = psonia
      // map each record to new with key the groceries names in order and value 1
      .map(line => {
        var columns = line.split(",").sorted
      for {     //Για κάθε στοιχείο του πίνακα θα πρέπει να συνδυαστεί με τα υπόλοιπα
          n <-0 to (columns.length-1)    //τα πρώτα θα συνδιαστούν
          c <-1+n to (columns.length-1)   //με τα επόμενα
        }
        yield (columns(n) + "+" + columns(c))
      })
     .filter(_.nonEmpty)

    val Tripla = psonia
      // map each record to new with key the groceries names in order and value 1
      .map(line => {
        var columns = line.split(",").sorted
        for {     //For each value of table need to pair with rest - Για κάθε στοιχείο του πίνακα θα πρέπει να συνδυαστεί με τα υπόλοιπα
          n <-0 to (columns.length-1)    //first will pair - τα πρώτα θα συνδιαστούν
          c <-1+n to (columns.length-1)   //with next -με τα επόμενα
          f <-1+n+c to (columns.length-1)   //with next -με τα επόμενα
        }
        yield
          //if (columns.length>2)
          (columns(n) + "+" + columns(c) + "+" + columns(f))
      })
      .filter(_.nonEmpty)


      // count the psonia
    printf("-- Lines with Pairs -----: %d\n",Dipla.count())

    //println(Dipla.getClass) //debuging
    //println(Dipla.first().getClass)  //debuging

    val ColumnZeygi=Dipla //counting pairs - ώρα να μετρηθούν τα  ζευγάρια
      .flatMap(line => (line.map(z => (z)))) //finding compinations - ανιχνεύω τους συνδιασμούς
      .map(line=> (line,1)) //recognise schema - αναγνωρίζω το schema
      .reduceByKey(_+_) //counting καταμέτρηση
      .filter(_._2>=plithos)   //demanding filter - απαίτηση για παράμετρικό φιλτράρισμα αποτελεσμάτων
      .sortBy(_._2,false) //ordering - ταξινόμηση

    ColumnZeygi.take(6).foreach(println)
    //ColumnZeygi.saveAsTextFile ("data/exercise4/output/thema2_2-out")   //saving results - αποθήκευση αποτελεσμάτων


    printf("-- Lines with Triplets ----: %d\n",Tripla.count())
    val ColumnTriplets=Tripla //count triples - ώρα να μετρηθούν τα τριπλά
      .flatMap(line => (line.map(z => (z)))) //find compinations - ανιχνεύω τους συνδιασμούς
      .map(line=> (line,1)) //recognise schema - αναγνωρίζω το schema
      .reduceByKey(_+_) //count - καταμέτρηση
      .filter(_._2>=plithos) //filtering - απαίτηση για παράμετρικό φιλτράρισμα αποτελεσμάτων
      .sortBy(_._2,false) //ordering - ταξινόμηση

    ColumnTriplets.take(6).foreach(println)
    //ColumnTriplets.saveAsTextFile ("data/exercise4/output/thema2_3-out")   //saving results - αποθήκευση αποτελεσμάτων
  }
}
