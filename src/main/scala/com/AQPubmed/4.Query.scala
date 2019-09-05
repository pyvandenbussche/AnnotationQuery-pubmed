package com.AQPubmed

// Databricks notebook source
import com.AQPubmed.BuildParquet.{sc, sqlContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import com.elsevier.aq.query._
import com.elsevier.aq.annotations._
import com.elsevier.aq.concordancers._
import com.elsevier.aq.utilities._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Query extends App {
  val sc = new SparkContext(new SparkConf().setAppName("AnnotateSCNLP").setMaster("local[*]"))
  val sqlContext: SQLContext = new SQLContext(sc)
  import sqlContext.implicits._

  // Define file paths
  // PubMed document key file
  val dataRoot = "data/"
  val keyFile = dataRoot + "keys/"
  val strMnt = dataRoot + "/str"
  val parquetOMMnt = dataRoot + "parquet/om"
  val parquetSCNLPMnt = dataRoot + "parquet/scnlp"
  val parquetPubMnt = dataRoot + "parquet/pubtator"

  // Load annotations

  val aqOM = GetAQAnnotations(sqlContext.read.parquet(parquetOMMnt).as[CATAnnotation],
    props=Array("*"), // load all properties
    decodeProps=Array("*")) // url-decode all
    .persist(StorageLevel.DISK_ONLY)

  val aqPUB = GetAQAnnotations(sqlContext.read.parquet(parquetPubMnt).as[CATAnnotation],
    props=Array("*"), // load all properties
    decodeProps=Array("*")) // url-decode all
    .persist(StorageLevel.DISK_ONLY)

  val aqSCNLP = GetAQAnnotations(sqlContext.read.parquet(parquetSCNLPMnt).as[CATAnnotation],
    props=Array("*"), // load all properties
    decodeProps=Array("*")) // url-decode all
    .persist(StorageLevel.DISK_ONLY)

  println("Number of Pubmed Documents: " + FilterType(aqOM,"document").count)
  println("Number of OM annotations: " + aqOM.count)
  println("Number of Pubtator annotations: " + aqPUB.count)
  println("Number of SCNLP annotations: " + aqSCNLP.count)

  /* ###############################
  Distribution of documents per year
  ################################## */

  val doc_years_annot = FilterType(aqOM,"document")
  val doc_years = doc_years_annot.select($"docId" +: Array("year").map(x => $"properties".getItem(x).alias(x)): _*)
  println(doc_years
    .groupBy($"year")
    .count()
    .orderBy($"year").show(5))

  /* ###############################
  Distribution of Genes in title and abstract sections
  ################################## */

val title_genes_annot = ContainedIn(FilterType(aqPUB,"gene"), FilterType(aqOM,"title"))
println("Number of results: " + title_genes_annot.count)
println(title_genes_annot)

val title_genes = title_genes_annot.select(Array("orig", "identifier").map(x => $"properties".getItem(x).alias(x)): _*)
println(title_genes
  .groupBy($"identifier")
  .agg(
    collect_set("orig") as "labels",
    count("identifier").alias("count")
  )
  .orderBy(desc("count")).show(5)
)
println(title_genes.count)

val abstract_genes_annot = ContainedIn(FilterType(aqPUB,"gene"), FilterType(aqOM,"abstract"))
println("Number of results: " + abstract_genes_annot.count)
println(abstract_genes_annot.show(5))

val abstract_genes = abstract_genes_annot.select(Array("orig", "identifier").map(x => $"properties".getItem(x).alias(x)): _*)
println(abstract_genes
  .groupBy($"identifier")
  .agg(
    collect_set("orig") as "labels",
    count("identifier").alias("count")
  )
  .orderBy(desc("count")).show(5)
)
println(abstract_genes.count)


  /* ###############################
  Cooccurrences of BRCA1 and BRCA2 with other genes in the same sentence
  ################################## */

// val cooc_brca1_annot = Contains(FilterType(aqSCNLP,"sentence"), FilterProperty(aqPUB,"identifier","672"))
val cooc_brca1_annot = ContainedIn(FilterType(aqPUB,"gene"),Contains(FilterType(aqSCNLP,"sentence"), FilterProperty(aqPUB,"identifier","672")) )
// display(cooc_brca1_annot)
val cooc_brca1 = cooc_brca1_annot.select(Array("orig", "identifier").map(x => $"properties".getItem(x).alias(x)): _*)
println(cooc_brca1
  .groupBy($"identifier")
  .agg(
    collect_set("orig") as "labels",
    count("identifier").alias("count")
  )
  .orderBy(desc("count")).show(5)
)
println(cooc_brca1.count)

val cooc_brca2_annot = ContainedIn(FilterType(aqPUB,"gene"),Contains(FilterType(aqSCNLP,"sentence"), FilterProperty(aqPUB,"identifier","675")) )
// display(cooc_brca2_annot)
val cooc_brca2 = cooc_brca2_annot.select(Array("orig", "identifier").map(x => $"properties".getItem(x).alias(x)): _*)
println(cooc_brca2
  .groupBy($"identifier")
  .agg(
    collect_set("orig") as "labels",
    count("identifier").alias("count")
  )
  .orderBy(desc("count")).show(5)
)
println(cooc_brca2.count)

  /* ###############################
  Cooccurrences of BRCA1 or BRCA2 with cell lines in the same sentence
  ################################## */

val cooc_brca_annot = ContainedIn(FilterType(aqPUB,"cellline"),Contains(FilterType(aqSCNLP,"sentence"), FilterProperty(aqPUB,"identifier",valueArr=Array("675","672")) ))
println(cooc_brca_annot.show(5))
val cooc_brca = cooc_brca_annot.select(Array("orig", "identifier").map(x => $"properties".getItem(x).alias(x)): _*)
println(cooc_brca
  .groupBy($"identifier")
  .agg(
    collect_set("orig") as "labels",
    count("identifier").alias("count")
  )
  .orderBy(desc("count")).show(5)
)
println(cooc_brca.count)

  /* ###############################
    Example of Hydrate function
    ################################## */

val hdratedAnnots = Hydrate(cooc_brca_annot,strMnt)
println(hdratedAnnots.show(5))

  /* ###############################
    Example of Concordancer
    ################################## */

val concordancerResult = XMLConcordancer(cooc_brca_annot,strMnt,FilterType(aqPUB,"gene"),10)
  println(concordancerResult)

// COMMAND ----------



}
