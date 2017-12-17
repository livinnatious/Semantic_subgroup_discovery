package net.sansa_stack.template.spark.rdf

import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.functions._
import java.net.URI
import scala.collection.mutable
import scala.text
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.rdd.RDD

object token2 {
    def main(args: Array[String]) = {
      val spark = SparkSession.builder
         .master("local[*]")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .appName("ML example")
         .getOrCreate()
      import spark.implicits._
      val sentenceDataFrame = spark.createDataFrame(Seq(
        (0, "Hi I heard about Spark"),
        (1, "I wish Java could use case classes"),
        (2, "Logistic,regression,models,are,neat")
      )).toDF("id", "sentence")
      
      val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
      val regexTokenizer = new RegexTokenizer()
        .setInputCol("sentence")
        .setOutputCol("words")
        .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)
      
      val countTokens = udf { (words: Seq[String]) => words.length }
      
      val tokenized = tokenizer.transform(sentenceDataFrame)
      tokenized.select("sentence", "words")
          .withColumn("tokens", countTokens(col("words"))).show(false)
      
      val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
      regexTokenized.select("sentence", "words")
          .withColumn("tokens", countTokens(col("words"))).show(false)
      }
}