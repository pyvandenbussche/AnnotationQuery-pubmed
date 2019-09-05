package com.AQPubmed

import java.io.File
import java.util.Properties
import org.apache.commons.io.FileUtils

import scala.collection.JavaConverters._
import edu.stanford.nlp.ling.CoreAnnotations._
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.util.CoreMap
import org.apache.spark.{SparkConf, SparkContext}


object AnnotateSCNLP extends App{
  val sc = new SparkContext(new SparkConf().setAppName("AnnotateSCNLP").setMaster("local[*]"))

  // Define file paths
  // PubMed document key file
  val dataRoot = "data/"
  val keyFile = dataRoot + "keys/"
  // folder with the normalized string
  val strMnt = dataRoot + "str/"
  // folder with the original markup annotations
  val scnlpAnnotMnt = dataRoot + "scnlp/"

  // Number of partitions (parallelization)
  val numParts = 4

  // After reading the list of pubmed IDs we are intersted in, we parrallelize the calls to pubtator and store the XML results
  // Read the key file
  val results =  sc.textFile(keyFile).repartition(numParts)
    .mapPartitions(keyIter => {
      // Create SCNLP pipeline to be used by all workers
      val props: Properties = new Properties()
      props.put("annotators", "tokenize, ssplit")
      val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

      keyIter.map(key => {
        try {
          var rawStr = FileUtils.readFileToString(new File(strMnt + key), "UTF-8")

          // get the sentences contained in the raw string (title and abstract from pubmed)
          val scnlp_annotation: Annotation = pipeline.process(rawStr)
          val sentences = scnlp_annotation.get(classOf[SentencesAnnotation]).asScala.toList

          // write annotations in the caret format, e.g. 1^scnlp^sentence^0^1635^origAnnotID=1"
          val annotations = (for {sentence: CoreMap <- sentences} yield (Array(
            sentence.get(classOf[SentenceIndexAnnotation]),
            "scnlp",
            "sentence",
            sentence.get(classOf[CharacterOffsetBeginAnnotation]),
            sentence.get(classOf[CharacterOffsetEndAnnotation]),
            "origAnnotID="+sentence.get(classOf[SentenceIndexAnnotation])
          ).mkString("^"))).mkString("\n")

          FileUtils.writeStringToFile(new File(scnlpAnnotMnt + key), annotations, "UTF-8")

          (key,"S")
        } catch {
          case e: Exception => {
            e.printStackTrace()
            (key,"F")
          }
        }
      })
    }).cache
  println("Total Processed: " + results.count) // Action to force execution
  println("Success: " + results.filter(rec => rec._2 == "S").count)
  println("Failed: " + results.filter(rec => rec._2 == "F").count)


  // Let's have a look at one such file containing the sentence annotations from SCNLP
  val sampleKey = sc.textFile(keyFile).take(1)(0)
  println("\nExample SCNLP annotation file:")
  println(FileUtils.readFileToString(new File(scnlpAnnotMnt + sampleKey), "UTF-8"))

  // COMMAND ----------




}
