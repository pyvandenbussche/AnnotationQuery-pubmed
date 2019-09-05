name := "Pubmed Annotation Query using Spark Scala"
version := "1.0"
scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.4.1",
  "org.apache.spark" % "spark-sql_2.11" % "2.4.1",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2"
)