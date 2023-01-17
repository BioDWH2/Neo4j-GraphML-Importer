![Java CI](https://github.com/BioDWH2/Neo4j-GraphML-Importer/workflows/Java%20CI/badge.svg?branch=develop) ![Release](https://img.shields.io/github/v/release/BioDWH2/Neo4j-GraphML-Importer) ![Downloads](https://img.shields.io/github/downloads/BioDWH2/Neo4j-GraphML-Importer/total) ![License](https://img.shields.io/github/license/BioDWH2/Neo4j-GraphML-Importer)

# Neo4j-GraphML-Importer
**BioDWH2** is an easy-to-use, automated, graph-based data warehouse and mapping tool for bioinformatics and medical informatics. The main repository can be found [here](https://github.com/BioDWH2/BioDWH2).

This repository contains the **Neo4j-GraphML-Importer** utility for importing GraphML files into existing Neo4j databases. Because current versions of the Neo4j APOC-plugin can't handle large GraphML files during import, this tool was developed. The GraphML format strictly follows the Neo4j APOC schema used for export and import. An example of the format is available below.

## Download
The latest release version of **Neo4j-GraphML-Importer** can be downloaded [here](https://github.com/BioDWH2/Neo4j-GraphML-Importer/releases/latest).

## Usage
Neo4j-GraphML-Importer requires the Java Runtime Environment version 16 or higher. The JRE 16 is available [here](https://adoptopenjdk.net/releases.html?variant=openjdk16).

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
Usage: Neo4j-GraphML-Importer.jar [-h] [-i=<graphMLFilePath>] [-e=<endpoint>]
                                  [--username=<username>] [--password=<password>]
                                  [--label-prefix=<prefix>] [--label-suffix=<suffix>]
                                  [--modify-edge-labels=<true|false>]
                                  [--modify-node-labels=<true|false>]
                                  [--indices=<label1>.<property1>;<label2>.<property2>;...]
  -h, --help                      print this message
  -i, --input=<graphMLFilePath>   Path to the GraphML file
  -e, --endpoint=<endpoint>       Endpoint of a running Neo4j instance
      --username=<username>       Neo4j username
      --password=<password>       Neo4j password
      --label-prefix=<prefix>     Prefix added to all node and edge labels
      --label-suffix=<suffix>     Suffix added to all node and edge labels
      --modify-node-labels=<true|false>
               Flag if node labels should be modified with prefix and suffix. Default: true
      --modify-edge-labels=<true|false>
               Flag if edge labels should be modified with prefix and suffix. Default: true
      --indices=<label1>.<property1>;<label2>.<property2>;...
               Create indices if not exist. Prefix and suffix are not automatically added to these labels!
~~~

## GraphML format
Example of the GraphML format usable for Neo4j import:
~~~xml
<?xml version='1.0' encoding='UTF-8'?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
    <key id="nt0" for="node" attr.name="names" attr.list="string"
         attr.type="string"/>
    <key id="nt1" for="node" attr.name="attitude" attr.type="string"/>
    <key id="nt2" for="node" attr.name="age" attr.type="long"/>
    <key id="labels" for="node" attr.name="labels" attr.type="string"/>
    <key id="label" for="edge" attr.name="label" attr.type="string"/>
    <key id="et0" for="edge" attr.name="reason" attr.type="string"/>
    <graph id="G" edgedefault="directed">
        <node id="n1" labels=":Animal:Cat">
            <data key="labels">:Animal:Cat</data>
            <data key="nt0">["Mr. Snuffels", "The cat next door"]</data>
            <data key="nt1">playful</data>
            <data key="nt2">4</data>
        </node>
        <node id="n2" labels=":Animal:Mouse">
            <data key="labels">:Animal:Mouse</data>
            <data key="nt1">shy</data>
            <data key="nt2">1</data>
        </node>
        <edge id="e1" source="n1" target="n2" label="CHASES">
            <data key="label">CHASES</data>
            <data key="et0">hunger</data>
        </edge>
    </graph>
</graphml>
~~~
