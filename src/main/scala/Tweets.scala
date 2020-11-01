import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.StringIndexer


object Tweets {
  def main(args: Array[String]) = {
    println("Hello World!")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val inputFilePath = "Tweets.csv"
    val inputDF = spark.read.option("header", "true").csv(inputFilePath).toDF()
    inputDF.printSchema()
    inputDF.show(false)

    val cleanData = inputDF.drop("airline_sentiment_confidence", "negativereason", "negativereason_confidence", "airline",
      "airline_sentiment_gold", "name", "negativereason_gold", "retweet_count", "tweet_coord", "tweet_created", "tweet_location", "user_timezone")
    cleanData.show()

    val tweetsDF = cleanData.filter(cleanData.col("text").isNotNull).toDF("id", "StringLabel", "text")
    tweetsDF.show()

    val indexer = new StringIndexer().setInputCol("StringLabel").setOutputCol("label").fit(tweetsDF)
    val tweetsDFIndexed = indexer.transform(tweetsDF)
    tweetsDFIndexed.show()

    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val stopWordsRemover = new StopWordsRemover().setInputCol(tokenizer.getOutputCol).setOutputCol("filtered")
    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(stopWordsRemover.getOutputCol).setOutputCol("features")
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.001)
    val pipeline = new Pipeline().setStages(Array(tokenizer, stopWordsRemover, hashingTF, lr))

    

    val model = pipeline.fit(tweetsDFIndexed)


  }

}
