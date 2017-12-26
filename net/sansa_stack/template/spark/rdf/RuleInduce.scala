////////////////////////////////////////////////
// Authors: Livin Natious, Pardeep Kumar Naik //
// Created on: 12/12/2017                     //
// Version: 0.0.1                             //
// Efficient Subgroup discovery using Spark   //
////////////////////////////////////////////////

package net.sansa_stack.template.spark.rdf

import java.net.URI
import scala.collection.mutable
import scala.collection.mutable.Map
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.jena.graph.Triple
import org.apache.spark.sql.functions._

class RuleInduce(dataSetDF: DataFrame, ontRDD: Array[RDD[Triple]], dictDF: DataFrame, spark: SparkSession) extends Serializable{
  
  //Config values wrt to ip dataset
  //minimum size(threshold) of interesting subgroup
  val MIN_SIZE = 4 
  //max terms in the rule(model)
  val MAX_TERMS = 4
  //max ontologies allowed(as per SEGS)
  val MAX_ONT = 4
  //Subgroup column and class
  val sgCol = "big_spender"
  val sgClass = "Yes"
  //ontology(index of args) to dataset(column) mapping
  val ontMap = Map( 0 -> List("occupation"),
        1 -> List("location"), 
        2 -> List("account", "loan", "deposit", "investment_fund", "insurance"))
  
  // ruleCnd and ruleCndC to calc WRAcc of rule
  var ruleCnd = Map[Map[Int, String], Long]()
  var ruleCndC = Map[Map[Int, String], Long]()
  
  val ont = spark.sparkContext.broadcast(ontRDD)
  
  def run(){
    
    //construct(null, ontRDD(0), 0)
    //println(descendants("BankingService",0))
    //construct(Map(0 -> "Health", 1 -> "Poznan"), "", 0)
    //println(ruleCompat(Map(2 -> "Gold")))
        
    descendantsRDD(0)
  }
  
  //rule construction method; 3 inputs: current rule, concept of ontology 'k', ontology index 'k' 
  def construct(rule: Map[Int, String], concept: String, k: Int){
    //TO-DO

    val newSetDF = null;
    //println(dataSetDF.columns(0))
    //ontRDD(0).take(1).foreach(println)
    ruleSetDF(rule).show
    
  }
  
  //function to get the DF rows related to the rule
  def ruleSetDF(rule: Map[Int, String]): DataFrame = {
    //TO-DO
    val filDF: Array[DataFrame] = new Array[DataFrame](rule.size)
    rule.zipWithIndex.foreach({case(r, i) => filDF(i) = conceptSetDF(r._2, r._1)})
    val ruleDF = intersectionDF(filDF).cache
    ruleCnd(rule) = ruleDF.count
    ruleCndC(rule) = ruleDF.filter(col(sgCol).like(sgClass)).count
    ruleDF
  }
  
  //function to get the DF rows related to the concept
  def conceptSetDF(concept: String, k: Int): DataFrame = {
    //TO-DO
    val concepts = List(concept) ++ descendants(concept, k)
    val cartSize = concepts.size * ontMap(k).size
    val filDF: Array[DataFrame] = new Array[DataFrame](cartSize)
    var i = 0
    ontMap(k).foreach(f=> concepts.foreach(x => {filDF(i) = dataSetDF.filter(col(f).like(x)); i+=1}))
    unionDF(filDF).distinct
  }
  
  //to find the immediate child/children concept of concept
  def children(concept: String, k: Int): Array[String] = {
    //TO-DO
    return null
  }
  
  def descendants(concept: String, k: Int): List[String] = {
    val childRDD = ontRDD(k).filter(f => {f.getObject.toString.contains(concept)}).map(f => f.getSubject.toString.split("#").last)
    var childList = childRDD.collect.toList
    childList.foreach(f => childList = childList ++ descendants(f , k))
    childList
  }
  
  def intersectionDF( listDF : Seq[DataFrame]): DataFrame = {
    listDF.reduce((x,y)=> x.intersect(y).coalesce(2))
  }
  
  def unionDF( listDF : Seq[DataFrame]): DataFrame = {
    listDF.reduce((x,y)=> x.union(y).coalesce(2))
  }
  
  def descendantsRDD(k: Int){
    val conceptRDD = ont.value(k).map(f => f.getSubject.toString).union(ont.value(k).map(f => f.getObject.toString)).distinct
    val descRDD = conceptRDD.map(f => {
      (f,getDescendants(f,k))
      })
    val filDescRDD = descRDD.filter(f => f._2.nonEmpty)
    filDescRDD.take(10).foreach(println)
  }
  
  def getDescendants(concept: String, k: Int): List[String] = {
    val childRDD = ont.value(k).filter(f => {f.getObject.toString.contains(concept)}).map(f => f.getSubject.toString)
    var childList = childRDD.collect.toList
    childList.foreach(f => childList = childList ++ getDescendants(f , k))
    childList
  }
}