# Querying text annotations at scale with SPARK

[![Documentation Status](https://img.shields.io/badge/Blog-link_to_the_post-brightgreen.svg)]( http://pyvandenbussche.info/2019/querying-text-annotations-at-scale-with-spark/)

Experiment using Elsevier Labs' [Annotation Query](https://github.com/elsevierlabs-os/AnnotationQuery) library to query 
annotations of PubMed articles. The code is in Scala and leverage SPARK processing.
 
The annotations for the articles are extracted from [PubTator](https://www.ncbi.nlm.nih.gov/research/pubtator/index.html) 
and [Stanford Core NLP](https://stanfordnlp.github.io/CoreNLP/). 

## Installation

Use build.sbt to install dependencies.

You need to compile (`mvn package`) and add the jar for the two following libraries:
- [Annotation Query](https://github.com/elsevierlabs-os/AnnotationQuery)
- [spark-xml-utils](https://github.com/elsevierlabs-os/spark-xml-utils)

## Running the code
