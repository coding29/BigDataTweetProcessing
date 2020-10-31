import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Tweets {
  def main(args: Array[String]): Unit = {
    println("Hello World!")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val inputFilePath = "Tweets.csv"
    val tweetsData = spark.read.format("csv").option("header", "true").load(inputFilePath)

    tweetsData.show(20)


  }

}
