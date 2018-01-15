////////////////////////////////////////////////
// Authors: Livin Natious, Pardeep Kumar Naik //
// Created on: 01/12/2017                     //
// Version: 0.0.1                             //
// Efficient Subgroup discovery using Spark   //
////////////////////////////////////////////////

package net.sansa_stack.template.spark.rdf
import scala.collection.mutable.ArrayBuffer

import java.net.URI
import scala.collection.mutable
import scala.collection.mutable.Map
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.jena.graph.Triple
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer

class RuleInduce1(dataSetDF: DataFrame, ontRDD: Array[RDD[Triple]], dictDF: DataFrame, spark: SparkSession) extends Serializable
{
  
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
      var colDataSetDF = dataSetDF.withColumn("counter", lit(5))
  def run()
  {
    
    //construct(null, ontRDD(0), 0)
    //println(descendants("BankingService",0))
   // construct(Map(0 -> "Health", 1 -> "Poznan"), "", 0)
    //println(ruleCompat(Map(2 -> "Gold")))
    ruleConstruction()
  }
  
  def ruleConstruction()
  { 

    val ruleSet:ListBuffer[Map[Int, String]] = ListBuffer(Map(0 -> "Doctor", 1 -> "Rome"),
        Map(0 -> "Doctor", 1 -> "Frankfurt", 2 -> "Deposit"),
        Map(0 -> "Police", 2 -> "Gold"), 
        Map(0 -> "Police", 1 -> "Munich", 2 -> "Gold"))
        
    val ruleSetWRAcc:ListBuffer[Map[Map[Int, String], Double]] = ListBuffer(Map(ruleSet(0) -> 5.92),
        Map(ruleSet(1) -> 4.14),
        Map(ruleSet(2) -> 4.92),
        Map(ruleSet(3) -> 1.04))
        
    var sortRuleSetWRAcc = ruleSetWRAcc.sortBy(x=>x.values.max).reverse

    var i = 0
    val bestRuleSet = ArrayBuffer[Map[Int, String]]()
        do
        {
          
           val bestRule = getBestRule(sortRuleSetWRAcc)
           println("bestrule: " + bestRule)
           println("---------")
           decreaseCount(bestRule)
           val tempList = sortRuleSetWRAcc.slice(1, sortRuleSetWRAcc.length)
           sortRuleSetWRAcc = tempList
           bestRuleSet += bestRule
           println("bestRuleSet:")
           bestRuleSet.foreach(x => {println(x)})
           println("---------")
           i=i+1
         } while(colDataSetDF == null || i < ruleSet.length) 
    }    
   

 def getBestRule(sortRuleSetWRAcc: ListBuffer[Map[Map[Int, String], Double]]): Map[Int, String] =
  {
   
     val bestRule = sortRuleSetWRAcc(0).keys.head
     bestRule 
  }
  def decreaseCount(bestRule: Map[Int, String]) 
  {
      import spark.implicits._
      val WRADF = ruleSetDF(bestRule, colDataSetDF)
      if (WRADF.count() == 0)
        return
      val removeWRADFRow = colDataSetDF.except(WRADF).coalesce(2) 
      val decrementCounterUDF = udf((decrementCounter:Int) => decrementCounter-1) 
      val newDF = WRADF.withColumn("counter", decrementCounterUDF($"counter"))
      colDataSetDF = removeWRADFRow.union(newDF).filter($"counter">=4).coalesce(2)
  }
 /* def decrementCounter(row: Row): Int ={
    
          row.get(9).toString.toInt-1
  } */
  //rule construction method; 3 inputs: current rule, concept of ontology 'k', ontology index 'k' 
/*  def construct(rule: Map[Int, String], concept: String, k: Int)
  {
    //TO-DO

    val newSetDF = null;
    //println(dataSetDF.columns(0))
    //ontRDD(0).take(1).foreach(println)
   // ruleSetDF(rule).show
    
  }*/
  
  //function to get the DF rows related to the rule
  def ruleSetDF(rule: Map[Int, String], colDataSetDF: DataFrame): DataFrame = 
  {
    //TO-DO
    val filDF: Array[DataFrame] = new Array[DataFrame](rule.size)
    rule.zipWithIndex.foreach({case(r, i) => filDF(i) = conceptSetDF(r._2, r._1, colDataSetDF)})
      val ruleDF = intersectionDF(filDF).cache
 //   ruleCnd(rule) = ruleDF.count
 //   ruleCndC(rule) = ruleDF.filter(col(sgCol).like(sgClass)).count
      ruleDF
  }
 
  //function to get the DF rows related to the concept
  def conceptSetDF(concept: String, k: Int, colDataSetDF: DataFrame): DataFrame = 
  {
    //TO-DO
    val concepts = List(concept) ++ descendants(concept, k)
    val cartSize = concepts.size * ontMap(k).size
    val filDF: Array[DataFrame] = new Array[DataFrame](cartSize)
    var i = 0
    ontMap(k).foreach(f=> concepts.foreach(x => {filDF(i) = colDataSetDF.filter(col(f).like(x)); i+=1}))
    unionDF(filDF).distinct
  }
  
  def descendants(concept: String, k: Int): List[String] = 
  {
    val childRDD = ontRDD(k).filter(f => {f.getObject.toString.contains(concept)}).map(f => f.getSubject.toString.split("#").last)
    var childList = childRDD.collect.toList
    childList.foreach(f => childList = childList ++ descendants(f , k))
    childList
  }
  
  def intersectionDF( listDF : Seq[DataFrame]): DataFrame = 
  {
    listDF.reduce((x,y)=> x.intersect(y).coalesce(2))
  }
  
  def unionDF( listDF : Seq[DataFrame]): DataFrame = 
  {
    listDF.reduce((x,y)=> x.union(y).coalesce(2))
  }
}