package net.sansa_stack.template.spark.rdf

import java.net.URI
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader

object tfidf_nt {
def main(args: Array[String]) = {
   val input = "src/main/resources/page_links_simple.nt" // args(0)
   println("======================================")
   println("|        Triple reader example       |")
   println("======================================")
   val sparkSession = SparkSession.builder
     .master("local[*]")
     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     .appName("WordCount example (" + input + ")")
     .getOrCreate()
 
   val triples = sparkSession.sparkContext.textFile(input)
   triples.take(5).foreach(println(_))
   triples.cache()
  
   val nrOfTriples = triples.count()
   println("\nCount: " + nrOfTriples)

   val removeCommentRows = triples.filter(!_.startsWith("#"))
   //removeCommentRows.take(5).foreach(println(_))

   val parsedTriples = removeCommentRows.map(App.parsTriples)
   //parsedTriples.take(5).foreach(println(_))
   val subject = parsedTriples.map(_.subject)
   
   subject.take(5).foreach(println)
   
   sparkSession.stop
  }
}