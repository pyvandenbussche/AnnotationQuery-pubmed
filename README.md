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
The code is tightly bound to the following workflow: 
![Alt text](Overview.png?raw=true "Workflow overview")

1. ParsePubtatorXML: Running this app will parse the list of PubMed article IDs we are interested in 
(stored in `./data/keys`). and for each article query Pubtator and store the XML response in the `./data/xml` forlder.
Then for each XML file, we extract the string, original document markup and pubtator annotations:
    - `./data/str` contains the string content of the document stripped from any annotation (all annotation offsets referencing this text)
    - `./data/pubtator` contains the pubtator annotations including Gene, Disease, Chemical, Mutation, Species and CellLine
    - `./data/om` contains the original markup of the document including Document, Title and Abstract.
    
2. AnnotateSCNLP: This app is using Stanford Core NLP to annotate the sentences contained in each article text. 
The annotations are then stored in `./data/scnlp`

3. BuildParquet: This app store each annotation set (om, pubtator and scnlp) in a parquet file in a format specified by 
Annotation Query. 

4. Query: This app runs several scenarios querying the annotations with logical relations such as: 
*"give me all annotations of genes and cell lines that co-occur in the same sentence"*
