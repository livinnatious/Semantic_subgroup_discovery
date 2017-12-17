package net.sansa_stack.template.spark.rdf

import java.net.URI
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.LogisticRegression

object token1 {
    def main(args: Array[String]) = {
      val spark = SparkSession.builder
         .master("local[*]")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .appName("ML example")
         .getOrCreate()
      import spark.implicits._
      
      //sample training data
      val training = spark.createDataFrame(Seq(
        (0, "Hi I heard about Spark"),
        (1, "I wish Java could use case classes"),
        (2, "Logistic regression models are neat")
      )).toDF("label", "text")
      //sample test data
      val test = spark.createDataFrame(Seq(
        (0, "logistic is bad")
      )).toDF("label", "text")
      
      val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("word")
      val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
      val lr = new LogisticRegression().setMaxIter(10).setRegParam(.001)
      val pipeline = new Pipeline().setStages(Array(tokenizer,hashingTF,lr))
      //Estimator (here log regression is estimator);fit applide to get a model
      val model = pipeline.fit(training)
      //TRansformer (here model) transforms one dataframe to data frame with predictions
      val prediction = model.transform(test)
      //gets prediction results
      prediction.select( "label", "features","probability", "prediction").show(5, false)
      spark.stop
    }
}