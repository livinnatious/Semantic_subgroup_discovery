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
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.jena.graph.Triple
import org.apache.spark.sql.functions._

class RuleInduce(dataSetDF: DataFrame, ontRDD: Array[RDD[Triple]], dictDF: DataFrame, spark: SparkSession) extends Serializable{

  //Config values wrt to ip dataset
  //minimum size(threshold) of interesting subgroup
  val MIN_SIZE = 8
  //max terms in the rule(model)
  val MAX_TERMS = 4
  //max ontologies allowed(as per SEGS)
  val MAX_ONT = ontRDD.length
  //top concept of ontologies
  val TOP_CONCEPT = "Thing"
  //Subgroup column and class
  val sgCol = "big_spender"
  val sgClass = "Yes"
  //ontology(index of args) to dataset(column) mapping
  val ontMap = Map( 0 -> List("occupation"),
        1 -> List("location"), 
        2 -> List("account", "loan", "deposit", "investment_fund", "insurance"))
  
  // N(total records) and C(total subgrp records) to calc WRAcc of rule
  val N = dataSetDF.count
  val C = dataSetDF.filter(dataSetDF(sgCol) === sgClass).count     
  // ruleCnd and ruleCndC to calc WRAcc of rule
  var ruleCnd = Map[Map[Int, String], Long]()
  var ruleCndC = Map[Map[Int, String], Long]()
  
  var ruleSet = new ListBuffer[Map[Int, String]]()
  
  val ont = spark.sparkContext.broadcast(ontRDD)
  val descendantsRDD= new Array[RDD[(String, List[String])]](ontRDD.length);
  
  var colDataSetDF = dataSetDF.withColumn("counter", lit(5))
  
  def run(){
    //TO-DO
    for(i <- 0 until ontRDD.length)
      descendantsRDD(i) = genDescendantsRDD(i)
    construct(Map(), TOP_CONCEPT, 0)  
    println(ruleSet)
    val ruleSetWRAcc = ruleSet.map(rule => (rule, calcWRAcc(rule)))
    //ruleSelection(ruleSetWRAcc)
    println(ruleSetWRAcc)
  }
  
  //rule construction method; 3 inputs: current rule, concept of ontology 'k', ontology index 'k' 
  def construct(rule: Map[Int, String], concept: String, k: Int){
    //TO-DO
    println(ruleSet.size)
    if(ruleSet.size > 2){
      println(ruleSet.size)
      return
    }
    val allNewSetDF = ruleSetDF(rule).intersect(conceptSetDF(concept, k));
    val newSetDF = allNewSetDF.filter(dataSetDF(sgCol) === sgClass)
    //newSetDF.show
    if(newSetDF.count > MIN_SIZE){
      val ruleAdd = rule ++ Map( k -> concept)
      
      if(ruleAdd.size < MAX_TERMS && ruleAdd.size > 0){
        ruleSet += ruleAdd
        ruleCnd(ruleAdd) = allNewSetDF.count
        ruleCndC(ruleAdd) = newSetDF.count
      }
      
      if(ruleAdd.size < math.max(MAX_TERMS,MAX_ONT) && k < MAX_ONT-1){
        construct(ruleAdd, TOP_CONCEPT, k+1)
      
        val ruleMin = ruleAdd - k
        
        for( child <- getChildren(concept,k)){
          if(conceptSetDF(child, k).count > MIN_SIZE)
            construct(ruleMin, child, k)
        }
      }
    }    
    //println(dataSetDF.columns(0))
    //ontRDD(0).take(1).foreach(println)
    //println(getDescList(concept, k))
    //conceptSetDF(concept, k).show 
  }
  
//  def ruleSelection(ruleSetWRAcc: ListBuffer[(Map[Int, String], Double)]){ 
//        
//    var sortRuleSetWRAcc = ruleSetWRAcc.sortBy(x=>x.values.max).reverse
//
//    var i = 0
//    val bestRuleSet = ListBuffer[Map[Int, String]]()
//        do
//        {
//          
//           val bestRule = getBestRule(sortRuleSetWRAcc)
//           println("bestrule: " + bestRule)
//           println("---------")
//           decreaseCount(bestRule)
//           val tempList = sortRuleSetWRAcc.slice(1, sortRuleSetWRAcc.length)
//           sortRuleSetWRAcc = tempList
//           bestRuleSet += bestRule
//           println("bestRuleSet:")
//           bestRuleSet.foreach(x => {println(x)})
//           println("---------")
//           i=i+1
//         } while(colDataSetDF == null || i < ruleSet.length) 
//    } 
//  
//  def getBestRule(sortRuleSetWRAcc: ListBuffer[Map[Map[Int, String], Double]]): Map[Int, String] = {
//   
//     val bestRule = sortRuleSetWRAcc(0).keys.head
//     bestRule 
//  }
//  def decreaseCount(bestRule: Map[Int, String]){
//      import spark.implicits._
//      val WRADF = ruleSetDF(bestRule, colDataSetDF)
//      if (WRADF.count() == 0)
//        return
//      val removeWRADFRow = colDataSetDF.except(WRADF).coalesce(2) 
//      val decrementCounterUDF = udf((decrementCounter:Int) => decrementCounter-1) 
//      val newDF = WRADF.withColumn("counter", decrementCounterUDF($"counter"))
//      colDataSetDF = removeWRADFRow.union(newDF).filter($"counter">=4).coalesce(2)
//  }
  
  //function to get the DF rows related to the rule
  def ruleSetDF(rule: Map[Int, String]): DataFrame = {
    if(rule.isEmpty)
      return dataSetDF
    val filDF: Array[DataFrame] = new Array[DataFrame](rule.size)
    rule.zipWithIndex.foreach({case(r, i) => filDF(i) = conceptSetDF(r._2, r._1)})
    val ruleDF = intersectionDF(filDF).cache
    ruleDF
  }
  
  //function to get the DF rows related to the concept
  def conceptSetDF(concept: String, k: Int): DataFrame = {
    val concepts = List(concept) ++ getDescList(concept, k)
    val cartSize = concepts.size * ontMap(k).size
    val filDF: Array[DataFrame] = new Array[DataFrame](cartSize)
    var i = 0
    ontMap(k).foreach(f=> concepts.foreach(x => {filDF(i) = dataSetDF.filter(col(f).like(x)); i+=1}))
    unionDF(filDF).distinct
  }
  
//  def descendants(concept: String, k: Int): List[String] = {
//    val childRDD = ontRDD(k).filter(f => {f.getObject.toString.contains(concept)}).map(f => f.getSubject.toString.split("#").last)
//    var childList = childRDD.collect.toList
//    childList.foreach(f => childList = childList ++ descendants(f , k))
//    childList
//  }
  
  def intersectionDF( listDF : Seq[DataFrame]): DataFrame = {
    listDF.reduce((x,y)=> x.intersect(y).coalesce(2))
  }
  
  def unionDF( listDF : Seq[DataFrame]): DataFrame = {
    listDF.reduce((x,y)=> x.union(y).coalesce(2))
  }
  
  def genDescendantsRDD(k: Int): RDD[(String, List[String])] = {
    val conceptRDD = ont.value(k).map(f => f.getSubject.toString).union(ont.value(k).map(f => f.getObject.toString)).distinct
    val descRDD = conceptRDD.map(f => {(f,getRawDescList(f,k))})
    val wordfilDescRDD = descRDD.map( f => (f._1.split("#").last, f._2.map(x => x.split("#").last)))
    wordfilDescRDD
  }
  
  def getRawDescList(concept: String, k: Int): List[String] = {
    val childRDD = ont.value(k).filter(f => {f.getObject.toString.contains(concept)}).map(f => f.getSubject.toString)
    var childList = childRDD.collect.toList
    childList.foreach(f => childList = childList ++ getRawDescList(f , k))
    childList
  }
  
  def getDescList(concept: String, k: Int): List[String] = {
    val filRDD = descendantsRDD(k).filter(f => f._1.equals(concept))
    filRDD.first._2
  }
  
  //to find the immediate child/children concept of concept
  def getChildren(concept: String, k: Int): List[String] = {
    val childRDD = ont.value(k).filter(f => {f.getObject.toString.contains(concept)}).map(f => f.getSubject.toString)
    val childList = childRDD.collect.toList
    childList.map(x => x.split("#").last)
  }

  def calcWRAcc(rule: Map[Int, String]): Double = {
    //println("ruleCnd(rule):"+ruleCnd(rule)+", N:"+ N +", ruleCndC(rule)"+ruleCndC(rule)+", C:"+C)
    (ruleCnd(rule)/N.toDouble)*((ruleCndC(rule)/ruleCnd(rule).toDouble) - (C/N.toDouble))
  }
  
}