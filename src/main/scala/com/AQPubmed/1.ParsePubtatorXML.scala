package main.scala.com.AQPubmed

import java.io.File
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.io.FileUtils
import com.elsevier.spark_xml_utils.xquery.XQueryProcessor

object ParsePubtatorXML extends App{
  val sc = new SparkContext(new SparkConf().setAppName("ParsePubtatorXML").setMaster("local[*]"))

  // Define file paths
  // PubMed document key file
  val dataRoot = "data/"
  val keyFile = dataRoot + "keys/"
  // folder with original raw xml files
  val rawXMLMnt = dataRoot + "xml/"
  // folder with the normalized string
  val strMnt = dataRoot + "str/"
  // folder with the original markup annotations
  val omAnnotMnt = dataRoot + "om/"
  // folder with the pubtator annot files
  val pubtatorAnnotMnt = dataRoot + "pubtator/"

  // Number of partitions (parallelization)
  val numParts = 4

  // After reading the list of pubmed IDs we are intersted in, we parrallelize the calls to pubtator and store the XML results
  val results_rawXML =  sc.textFile(keyFile).repartition(numParts)
    .mapPartitions(keyIter => {
      keyIter.map(key => {
        try {
          // for each key, make a GET request to pubtator
          val url = "https://www.ncbi.nlm.nih.gov/research/pubtator-api/publications/export/biocxml?pmids=" + key
          val rawXML = scala.io.Source.fromURL(url).mkString
          // store the file in the raw mount
          FileUtils.writeStringToFile(new File(rawXMLMnt + key), rawXML, "UTF-8")
          (key,"S")
        } catch {
          case e: Exception => {
            e.printStackTrace()
            (key,"F")
          }
        }
      })
    }).cache
  println("Total Processed: " + results_rawXML.count) // Action to force execution
  println("Success: " + results_rawXML.filter(rec => rec._2 == "S").count)
  println("Failed: " + results_rawXML.filter(rec => rec._2 == "F").count)


  // We can check an XML response (corresponding to a pumed document ID) as provided by Pubtator
  println("Print a sample raw XML: ")
  val sampleKey = sc.textFile(keyFile).take(1)(0)
  println(FileUtils.readFileToString(new File(rawXMLMnt + sampleKey), "UTF-8"))

  /*
    We can now define some XQueries to extract:
    - str: the string content of the document stripped from any annotation (all annotation offsets referencing this text)
    - pubtator: the pubtator annotations including Gene, Disease, Chemical, Mutation, Species and CellLine
    - om: the original markup of the document including Document, Title and Abstract
   */
  val xquery_str = """
declare default element namespace "";

let $titlePassage := /collection/document/passage[infon[@key="type"] = "title"]
let $abstractPassage := /collection/document/passage[infon[@key="type"] = "abstract"]

return
  concat(string($titlePassage/text), " ",  string($abstractPassage/text))
"""

  val xquery_pubtator = """
declare default element namespace "";

let $annotations := /collection/document/passage/annotation
return
  string-join((for $annotation in $annotations
               let $annotId := string($annotation/@id)
               let $annotSet := "pubtator"
               let $annotType := lower-case(string($annotation/infon[@key="type"]))
               let $startOffset := string($annotation/location/@offset)
               let $endOffset := string(xs:integer($annotation/location/@offset) +  xs:integer($annotation/location/@length))
               let $orig := string($annotation/text)
               let $identifier := string($annotation/infon[@key="identifier"])
               let $attrs := concat("orig=",encode-for-uri($orig),"&amp;identifier=",encode-for-uri($identifier))
               return string-join(($annotId,$annotSet,$annotType,$startOffset,$endOffset,$attrs),"^")),codepoints-to-string(10))
"""

  val xquery_om = """
declare default element namespace "";

let $titlePassage := /collection/document/passage[infon[@key="type"] = "title"]
let $abstractPassage := /collection/document/passage[infon[@key="type"] = "abstract"]
let $passages := [$titlePassage, $abstractPassage]

let $annotSet := "section"

return

  string-join((
    string-join((
                let $text := string($abstractPassage/text)
                let $startTitleOffset := string(xs:integer($titlePassage/offset))
                let $startAbstractOffset := string(xs:integer($abstractPassage/offset))
                let $endOffset := string(xs:integer($startAbstractOffset) +  xs:integer(string-length($text)))
                let $startYearOffset := endOffset
                let $endYearOffset := string(xs:integer($endOffset) +  xs:integer(string-length($titlePassage/infon[@key="year"])))
                let $index := 1
                let $annotType := "document"
                let $attrs := concat("year=", string($titlePassage/infon[@key="year"]), "&amp;origAnnotID=",$index)

                return string-join(($index,$annotSet,$annotType,$startTitleOffset,$endOffset,$attrs),"^")),codepoints-to-string(10)),
    string-join((
                let $text := string($titlePassage/text)
                let $startOffset := string(xs:integer($titlePassage/offset))
                let $endOffset := string(xs:integer($startOffset) +  xs:integer(string-length($text)))
                let $index := 2
                let $annotType := string($titlePassage/infon[@key="type"])
                let $attrs := concat("parentId=1&amp;origAnnotID=",$index)

                return string-join(($index,$annotSet,$annotType,$startOffset,$endOffset,$attrs),"^")),codepoints-to-string(10)),
    string-join((
                let $text := string($abstractPassage/text)
                let $startOffset := string(xs:integer($abstractPassage/offset))
                let $endOffset := string(xs:integer($startOffset) +  xs:integer(string-length($text)))
                let $index := 3
                let $annotType := string($abstractPassage/infon[@key="type"])
                let $attrs := concat("parentId=1&amp;origAnnotID=",$index)

                return string-join(($index,$annotSet,$annotType,$startOffset,$endOffset,$attrs),"^")),codepoints-to-string(10))

    ),codepoints-to-string(10))
"""


  // Read the key file
  val results_annots =  sc.textFile(keyFile).repartition(numParts)
    .mapPartitions(keyIter => {
      val proc_pubtator = XQueryProcessor.getInstance(xquery_pubtator)
      proc_pubtator.setOutputMethod("text")
      val proc_om = XQueryProcessor.getInstance(xquery_om)
      proc_om.setOutputMethod("text")
      val proc_str = XQueryProcessor.getInstance(xquery_str)
      proc_str.setOutputMethod("text")

      keyIter.map(key => {
        try {
          var rawXML = FileUtils.readFileToString(new File(rawXMLMnt + key), "UTF-8")

          // Remove DocType declaration
          val cleanXML = rawXML.replaceAll("<!DOCTYPE(.)*><collection","<collection")

          val annot_pubtator = proc_pubtator.evaluateString(cleanXML)
          FileUtils.writeStringToFile(new File(pubtatorAnnotMnt + key), annot_pubtator, "UTF-8")

          val annot_om = proc_om.evaluateString(cleanXML)
          FileUtils.writeStringToFile(new File(omAnnotMnt + key), annot_om, "UTF-8")

          val annot_str = proc_str.evaluateString(cleanXML)
          FileUtils.writeStringToFile(new File(strMnt + key), annot_str, "UTF-8")

          (key,"S")
        } catch {
          case e: Exception => {
            e.printStackTrace()
            (key,"F")
          }
        }
      })
    }).cache
  println("Total Processed: " + results_annots.count) // Action to force execution
  println("Success: " + results_annots.filter(rec => rec._2 == "S").count)
  println("Failed: " + results_annots.filter(rec => rec._2 == "F").count)

  // Let's have a look at the results of these transformations. Let's get a string file
  // (containing the title, abstract and year of the document)
  println("\nExample string file:")
  println(FileUtils.readFileToString(new File(strMnt + sampleKey), "UTF-8"))

  println("\nExample pubtator annotation file:")
  println(FileUtils.readFileToString(new File(pubtatorAnnotMnt + sampleKey), "UTF-8"))

  println("\nExample original markup file file:")
  println(FileUtils.readFileToString(new File(omAnnotMnt + sampleKey), "UTF-8"))

}