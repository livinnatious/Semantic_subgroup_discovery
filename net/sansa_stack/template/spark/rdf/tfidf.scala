package net.sansa_stack.template.spark.rdf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

import java.net.URI
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.ml._
import org.apache.spark.ml.feature._

object tfidf{
      def main(args: Array[String]) = {
        val spark = SparkSession.builder
         .master("local[*]")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .appName("TF IDF example")
         .getOrCreate()
        import spark.implicits._
        val sentenceData = spark.createDataFrame(Seq(
          (0.0, "Hi I heard about Spark"),
          (1.0, "I wish Java could use case bloody"),
          (2.0, "Logistic regression models are neat")
        )).toDF("label", "sentence")
        
        val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
        val wordsData = tokenizer.transform(sentenceData)
        
        val hashingTF = new HashingTF()
          .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
        
        val featurizedData = hashingTF.transform(wordsData)
        // alternatively, CountVectorizer can also be used to get term frequency vectors
        
        val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        val idfModel = idf.fit(featurizedData)
        
        val rescaledData = idfModel.transform(featurizedData)
        println()
        rescaledData.select("label", "features").show(false)
  }
}