package net.sansa_stack.template.spark.rdf

import java.net.URI
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader

object App {
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
   println("Count: " + nrOfTriples)

   val removeCommentRows = triples.filter(!_.startsWith("#"))
   removeCommentRows.take(5).foreach(println(_))

   val parsedTriples = removeCommentRows.map(parsTriples)
   parsedTriples.take(5).foreach(println(_))
   val subject = parsedTriples.map(f => (f.subject, 1))

   val subjectCOunt = subject.reduceByKey(_ + _)
   println("SubjectCount \n")
   subjectCOunt.take(5).foreach(println(_))
    sparkSession.stop
}

def parsTriples(parsData: String): Triples = {
   val subRAngle = parsData.indexOf('>')
   val predLAngle = parsData.indexOf('<', subRAngle + 1)
   val predRAngle = parsData.indexOf('>', predLAngle + 1)
   var objLAngle = parsData.indexOf('<', predRAngle + 1)
   var objRAngle = parsData.indexOf('>', objLAngle + 1)

   if (objRAngle == -1) {
     objLAngle = parsData.indexOf('\"', objRAngle + 1)
     objRAngle = parsData.indexOf('\"', objLAngle + 1)
   }

   val subject = parsData.substring(1, subRAngle)
   val predicate = parsData.substring(predLAngle + 1, predRAngle)
   val `object` = parsData.substring(objLAngle + 1, objRAngle)

   Triples(subject, predicate, `object`)
 }

case class Triples(subject: String, predicate: String, `object`: String)
}