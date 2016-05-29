FindConnectedGraphs: A simple scala program to identify related entities
in an RDF graph via a specified predicate using Apache Spark's GraphX
library.

Usage:
======
# Set $SPARK_HOME to the location of your Spark distribution.
# Compile:
$ sbt package
# Run
$ $SPARK_HOME/bin/spark-submit   --class "findConnectedGraphs" --master local[4]   target/scala-2.10/findconnectedgraphs_2.10-1.0.jar 

TODO:
=====
- Clean up scala code
- Use the larger data set? There is currently an error...
