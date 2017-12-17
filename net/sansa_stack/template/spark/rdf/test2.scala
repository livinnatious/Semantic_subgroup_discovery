package net.sansa_stack.template.spark.rdf

import java.net.URI
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader

object Appln {
def main(args: Array[String]) = {
   val input = "src/main/resources/Cambridge.ntriples.nt" // args(0)
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
   //val objct = parsedTriples.map(f => (f.`object`, 1))

   //val objectCOunt = objct.reduceByKey(_ + _)
   //println("objectCount \n")
   //objectCOunt.take(5).foreach(println(_))
   
   //to get top 100 class
   
   val classSubject = parsedTriples.filter(x=>x.predicate.contains("#type")).filter(_.`object`.startsWith("http"))
   //classSubject.take(5).foreach(println(_))
   
   
   val objct = classSubject.map(f => (f.`object`, 1))
   val objectCOunt = objct.reduceByKey(_ + _)
   println("---------objectCount--------- \n")
   objectCOunt.take(5).foreach(println(_))
   
   
   val classSubjectCount = classSubject.map(f => (f.subject, 1)).reduceByKey(_ + _)
   
   println("---------Top 100 classes---------- \n")
   classSubjectCount.takeOrdered(5)(Ordering[Int].reverse.on(x=>x._2)).foreach(println(_))
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