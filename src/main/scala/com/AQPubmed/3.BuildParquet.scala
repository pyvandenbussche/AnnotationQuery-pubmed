package com.AQPubmed

import java.io.File
import org.apache.commons.io.FileUtils

import scala.collection.mutable.ListBuffer
import com.elsevier.aq.annotations.CATAnnotation
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext}


// create a parquet file from individual annotation files.
object BuildParquet extends App{
  val sc = new SparkContext(new SparkConf().setAppName("AnnotateSCNLP").setMaster("local[*]"))
  val sqlContext: SQLContext = new SQLContext(sc)

  // Define file paths
  // PubMed document key file
  val dataRoot = "data/"
  val keyFile = dataRoot + "keys/"
  val annotMntFolder = dataRoot
  val parquetMntFolder = dataRoot + "parquet/"

  val annotSets = Array("om", "pubtator", "scnlp")

  // Number of partitions (parallelization)
  val numParts = 4

  // for each annotation set
  for( annotSet <- annotSets ) {
    val annotMnt = annotMntFolder + annotSet + "/"
    val parquetMnt = parquetMntFolder + annotSet


    // Get the  annotation for each key and return (key,annotations)
    val annots = sc.textFile(keyFile).repartition(numParts).map(key => {
      (key,FileUtils.readFileToString(new File(annotMnt + key), "UTF-8"))
    })

    // Remove empty records, aborted records, ignored records
    val filteredAnnots = annots.filter(rec => rec._2.length > 0)
      .filter(rec => rec._2.startsWith("***") != true)

    // FlatpMap to to get all the annotations for each record
    val catAnnotations = filteredAnnots.flatMap(rec  => {
      var arr = rec._2.split("\n")
      val res = new ListBuffer[CATAnnotation]()
      for (i <- arr) {
        val parts = i.split("\\^")
        val docId = rec._1
        val annotSet = parts(1)
        val annotType = parts(2)
        val startOffset = parts(3).toLong
        val endOffset = parts(4).toLong
        val annotId = parts(0).toLong
        var other : String = null
        if (parts.size == 6) {
          other = parts(5)
        }
        res += CATAnnotation(docId,
          annotSet,
          annotType,
          startOffset,
          endOffset,
          annotId,
          if (other != null) Some(other) else None)
      }
      res
    })
    import sqlContext.implicits._
    // Write the parquet file
    catAnnotations.toDF().write.parquet(parquetMnt)
  }

  // COMMAND ----------

  // MAGIC %md Display the OM parquet file

  // COMMAND ----------

  val parquetMnt = parquetMntFolder + annotSets(0)
  println(sqlContext.read.parquet(parquetMnt))

  // COMMAND ----------



}
