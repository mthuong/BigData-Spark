package miu.cs522

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * BigData CS522 - Spark project
 *
 */
object Main {
  val FILENAME = "mtcars.csv"
  val TIMES = 1000
  val FRACTION = 0.25

  case class Cars(car: String, mpg: String, cyl: String, disp: String, hp: String,
                  drat: String,wt: String, qsec: String, vs: String, am: String, gear: String, carb: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("BigData CS522 - Spark project").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // Step 1: Read data from csv file
    import spark.implicits._
    val csv = sc.textFile(FILENAME)
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val mtcdata = headerAndRows.filter(_(0) != header(0))
    val mtcars = mtcdata
      .map(p => Cars(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11)))
      .toDF()
    mtcars.printSchema
    mtcars.show()

    import org.apache.spark.sql.functions._

    // Step 2: Select a categorical variable and a numeric variable and form the key-value pair and create a pairRDD called “population”.
    val population = mtcars.select("cyl", "mpg")
      .map(r => (r.getString(0), r.getString(1)))
      .cache()

    // Step 3
//    val roundingDouble = (d: Double) => BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

    val rdd = population.rdd
    population.show()

    val meanPopulation = rdd.mapValues(v => (v.toDouble, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(v => (v._1 / v._2, v._2))
      .mapValues(v => (v._1, v._2))
      .sortByKey()
      .cache()

    val variancePopulation = rdd.mapValues(v => v.toDouble).join(meanPopulation).map(v => (v._1, (Math.pow(v._2._1 - v._2._2._1, 2), v._2._2._2))
      )
      .reduceByKey((x, y) => (x._1 + y._1, x._2))
      .mapValues(v => v._1 / (v._2 - 1))
      .sortByKey()
      .cache()

    variancePopulation.toDF().show()

    val meanAndVariance = meanPopulation.mapValues(v => v._1).join(variancePopulation).cache()

    meanAndVariance.map(x => (x._1, x._2._1, x._2._2)).toDF("Category", "Mean", "Variance").show()

    // Step 4: Create the sample for bootstrapping. All you need to do is take 25% of the population without replacement.
    
  }
}
