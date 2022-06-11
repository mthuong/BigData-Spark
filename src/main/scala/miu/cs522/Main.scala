package miu.cs522

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
 * BigData CS522 - Spark project
 *
 */
object Main {
  val TIMES = 1000
  val FRACTION = 0.25

//  val FILENAME = "mtcars.csv"
//  val COLUMNS = Array("cyl", "mpg")
//  val MIN_KEYS = 3

  val FILENAME = "Fishing.csv"
  val COLUMNS = Array("mode", "catch")
  val MIN_KEYS = 4

  case class Cars(car: String, mpg: String, cyl: String, disp: String, hp: String,
                  drat: String,wt: String, qsec: String, vs: String, am: String, gear: String, carb: String)
  case class Fishing(mode: String, income: String)

  def calculateMeanAndVariance(spark: SparkSession, rdd: RDD[(String, String)]): RDD[(String, (Double, Double))] = {

    import spark.implicits._
    val meanPopulation = rdd.mapValues(v => (v.toDouble, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(v => (v._1 / v._2, v._2))
      .mapValues(v => (v._1, v._2))
      .sortByKey()
//      .cache()

    val variancePopulation = rdd.mapValues(v => v.toDouble).join(meanPopulation).map(v => (v._1, (Math.pow(v._2._1 - v._2._2._1, 2), v._2._2._2))
    )
      .reduceByKey((x, y) => (x._1 + y._1, x._2))
      .mapValues(v => {
        if (v._2 <= 1) {
          0
        } else {
          v._1 / (v._2 - 1)
        }
      })
      .sortByKey()
//      .cache()

    val meanAndVariance = meanPopulation.mapValues(v => v._1).join(variancePopulation)
//      .cache()
    meanAndVariance
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("BigData CS522 - Spark project").master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    sc.setLocalProperty("spark.scheduler.listenerbus.eventqueue.capacity", "100000")

    // Step 1: Read data from csv file
    import spark.implicits._
//    val csv = sc.textFile(FILENAME)
//    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
//    val header = headerAndRows.first
//    val data = headerAndRows.filter(_(0) != header(0))
//    val dataframe = data
//      .map(p => {
//        println(p(0))
//        if (DATASET == 2) {
//          Fishing(p(0), p(11))
//        } else {
//          Cars(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11))
//        }
//      })
//      .toDF()
//    dataframe.printSchema
//    dataframe.show()
    val dataframe = spark.read.option("header", true).csv(FILENAME).cache();
    dataframe.printSchema();
    dataframe.show();

    // Step 2: Select a categorical variable and a numeric variable and form the key-value pair and create a pairRDD called “population”.
    val roundingDouble = (d: Double) => BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    val population = dataframe.select("" + COLUMNS(0), "" + COLUMNS(1))
      .map(r => (r.getString(0), r.getString(1)))
      .cache()

    // Step 3: Compute the mean mpg and variance for each category
    val rdd = population.rdd
    population.toDF(COLUMNS(0), COLUMNS(1)).show()

    val meanAndVariance = calculateMeanAndVariance(spark, rdd)
    meanAndVariance.sortByKey().map(x => (x._1, roundingDouble(x._2._1), roundingDouble(x._2._2))).toDF("Category", "Mean", "Variance").show()

    // Step 4: Create the sample for bootstrapping. All you need to do is take 25% of the population without replacement.
    println("RDD count: " + rdd.count())
    var bootstrapSample = sc.emptyRDD[(String, String)]
    while(bootstrapSample.keys.distinct().count() < MIN_KEYS) {
      bootstrapSample = rdd.sample(false, FRACTION)
    }
    bootstrapSample.collect().foreach(print)
    println("\nBootStrapSample Keys: " + bootstrapSample.keys.distinct().collect().mkString(", "))

    // Step 5: Do 1000 times
    var sum = sc.emptyRDD[(String, (Double, Double))]
    for (i <- 1 to TIMES) {
      // 5a: Create a “resampledData”. All you need to do is take 100% of the sample with replacement
      val resampledData = bootstrapSample.sample(true, 1.0)
//      resampledData.toDF().show()

      // 5b: Compute the mean mpg and variance for each category
      val resampleMeanAndVariance = calculateMeanAndVariance(spark, resampledData)

      // 5c: Keep adding the values in some running sum.
      sum = sum.union(resampleMeanAndVariance).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      sum = sum.coalesce(10)
      sum.collect()

      if (i % 200 == 0)
        println("i: " + i)
    }
//    sum.map(s => (s._1, (roundingDouble(s._2._1), roundingDouble(s._2._2)))).sortByKey().toDF("Category", "Sum Mean and Variance").show()

    // Step 6: Divide each quantity by 1000 to get the average and display the result.
    val average = sum.map(v => (v._1, (v._2._1 / TIMES, v._2._2 / TIMES))).sortByKey().cache()
    average.map(v => (v._1, roundingDouble(v._2._1), roundingDouble(v._2._2))).toDF("Category", "Mean", "Variance").show()
  }
}
