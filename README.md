![Java CI](https://github.com/BioDWH2/Neo4j-GraphML-Importer/workflows/Java%20CI/badge.svg?branch=develop) ![Release](https://img.shields.io/github/v/release/BioDWH2/Neo4j-GraphML-Importer) ![Downloads](https://img.shields.io/github/downloads/BioDWH2/Neo4j-GraphML-Importer/total) ![License](https://img.shields.io/github/license/BioDWH2/Neo4j-GraphML-Importer)

# Neo4j-GraphML-Importer
**BioDWH2** is an easy-to-use, automated, graph-based data warehouse and mapping tool for bioinformatics and medical informatics. The main repository can be found [here](https://github.com/BioDWH2/BioDWH2).

This repository contains the **Neo4j-GraphML-Importer** utility for importing GraphML files into existing Neo4j databases. Because current versions of the Neo4j APOC-plugin can't handle large GraphML files during import, this tool was developed.

## Download
The latest release version of **Neo4j-GraphML-Importer** can be downloaded [here](https://github.com/BioDWH2/Neo4j-GraphML-Importer/releases/latest).

## Usage
Neo4j-GraphML-Importer requires the Java Runtime Environment version 8 or higher. The JRE 8 is available [here](https://www.oracle.com/java/technologies/javase-jre8-downloads.html).

Importing a GraphML file into Neo4j is done using the following command.
~~~BASH
> java -jar Neo4j-GraphML-Importer.jar -i /path/to/file.graphml -e bolt://localhost:8083 --username user --password pass
~~~

If authentication is disabled, the username and password parameters can be ignored.
~~~BASH
> java -jar Neo4j-GraphML-Importer.jar -i /path/to/file.graphml -e bolt://localhost:8083
~~~

## Help
~~~
Usage: Neo4j-GraphML-Importer.jar [-h] [-e=<endpoint>] [-i=<graphMLFilePath>]
                                  [--password=<password>]
                                  [--username=<username>]
  -e, --endpoint=<endpoint>       Endpoint of a running Neo4j instance
  -h, --help                      print this message
  -i, --input=<graphMLFilePath>   Path to the GraphML file
      --password=<password>       Neo4j password
      --username=<username>       Neo4j username
~~~
