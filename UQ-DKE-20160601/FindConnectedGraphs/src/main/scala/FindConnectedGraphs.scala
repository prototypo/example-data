/*//////////////////////////////////////////////////////////////////////////
Copyright 2016 David Hyland-Wood, based on code from Bob DuCharme.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
//////////////////////////////////////////////////////////////////////////*/

import scala.io.Source 
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap

object findConnectedGraphs {

    val componentLists = HashMap[VertexId, ListBuffer[VertexId]]()
    val labelMap =  HashMap[VertexId, String]()

    def main(args: Array[String]) {
        val sc = new SparkContext("local", "readLoCSH", "127.0.0.1")

        // regex pattern for end of triple
        val tripleEndingPattern = """\s*\.\s*$""".r
        // regex pattern for language tag
        val languageTagPattern = "@en".r    

        // Parameters of GraphX Edge are subject, object, and predicate
        // identifiers. RDF traditionally does (s, p, o) order but in GraphX
        // it's (edge start node, edge end node, edge description).

        var edgeArray = Array(Edge(0L,0L,"http://dummy/URI"))
        var literalPropsTriplesArray = new Array[(Long,Long,String)](0)
        var vertexArray = new Array[(Long,String)](0)

        // Read some data from a file
        //val source = Source.fromFile("data/n-triples/Go8.ntriples","UTF-8")
        val source = Source.fromFile("sampleSubjects.nt","UTF-8")

        val lines = source.getLines.toArray

        // When parsing the data we read, use this map to check whether each
        // URI has come up before.
        var vertexURIMap = new HashMap[String, Long];

        // Parse the data into triples.
        var triple = new Array[String](3)
        var nextVertexNum = 0L
        for (i <- 0 until lines.length) {
            // Space in next line needed for line after that. 
            lines(i) = tripleEndingPattern.replaceFirstIn(lines(i)," ")  
            triple = lines(i).mkString.split(">\\s+")       // split on "> "
            // Variables have the word "triple" in them because "object" 
            // by itself is a Scala keyword.
            val tripleSubject = triple(0).substring(1)   // substring() call
            val triplePredicate = triple(1).substring(1) // to remove "<"
            if (!(vertexURIMap.contains(tripleSubject))) {
                vertexURIMap(tripleSubject) = nextVertexNum
                nextVertexNum += 1
            }
            if (!(vertexURIMap.contains(triplePredicate))) {
                vertexURIMap(triplePredicate) = nextVertexNum
                nextVertexNum += 1
            }
            val subjectVertexNumber = vertexURIMap(tripleSubject)
            val predicateVertexNumber = vertexURIMap(triplePredicate)

            // If the first character of the third part is a <, it's a URI;
            // otherwise, a literal value. (Needs more code to account for
            // blank nodes.)
            if (triple(2)(0) == '<') { 
                val tripleObject = triple(2).stripPrefix("<").stripSuffix(">").trim // Lose < and >.
                if (!(vertexURIMap.contains(tripleObject))) {
                    vertexURIMap(tripleObject) = nextVertexNum
                    nextVertexNum += 1
                }
                val objectVertexNumber = vertexURIMap(tripleObject)
                edgeArray = edgeArray :+
                    Edge(subjectVertexNumber,objectVertexNumber,triplePredicate)
            }
            else {
                val tripleObjectTmp = triple(2).replace("\"", "") // Lose quotes around string.
                if ( tripleObjectTmp.indexOf("@en") > 0 && triplePredicate == "http://www.w3.org/2000/01/rdf-schema#label") {
                  val tripleObject =
                    languageTagPattern.replaceFirstIn(tripleObjectTmp,"") // Lose English language tag.
                  literalPropsTriplesArray = literalPropsTriplesArray :+
                      (subjectVertexNumber,predicateVertexNumber,tripleObject)
                }
            }
        }

        // Switch value and key for vertexArray that we'll use to create the
        // GraphX graph.
        for ((k, v) <- vertexURIMap) vertexArray = vertexArray :+  (v, k)   

        // We'll be looking up a lot of labels, so create a hashmap for
        // them. 
        for (i <- 0 until literalPropsTriplesArray.length) {
            if (literalPropsTriplesArray(i)._2 ==
                vertexURIMap("http://www.w3.org/2000/01/rdf-schema#label")) {
                labelMap(literalPropsTriplesArray(i)._1) = literalPropsTriplesArray(i)._3;
            }
        }

        // Create RDDs and Graph from the parsed data.

        // vertexRDD Long: the GraphX longint identifier. String: the URI.
        val vertexRDD: RDD[(Long, String)] = sc.parallelize(vertexArray)

        // edgeRDD String: the URI of the triple predicate. Trimming off the
        // first Edge in the array because it was only used to initialize it.
        val edgeRDD: RDD[Edge[(String)]] =
            sc.parallelize(edgeArray.slice(1,edgeArray.length))

        // literalPropsTriples Long, Long, and String: the subject and
        // predicate
        // vertex numbers and the the literal value that the predicate is
        // associating with the subject.
        val literalPropsTriplesRDD: RDD[(Long,Long,String)] =
            sc.parallelize(literalPropsTriplesArray)

        val graph: Graph[String, String] = Graph(vertexRDD, edgeRDD)

        // Create a subgraph based on the vertices connected by an
        // "affiliation" property.
        val affiliationRelatedSubgraph =
            graph.subgraph(t => t.attr ==
                           "http://dbpedia.org/ontology/affiliation")

        // Find connected components of affiliationRelatedSubgraph.
        val ccGraph = affiliationRelatedSubgraph.connectedComponents() 

        // Fill the componentLists hashmap.
        affiliationRelatedSubgraph.vertices.leftJoin(ccGraph.vertices) {
        case (id, u, comp) => comp.get
        }.foreach
        { case (id, startingNode) => 
          {
              // Add id to the list of components with a key of comp.get
              if (!(componentLists.contains(startingNode))) {
                  componentLists(startingNode) = new ListBuffer[VertexId]
              }
              componentLists(startingNode) += id
          }
        }

        // Output a report on the connected components. 
        println("------  connected components in related triples ------\n")
        for ((component, componentList) <- componentLists){
            if (componentList.size > 1) { // don't bother with lists of only 1
                for(c <- componentList) {
                    println(labelMap(c));
                }
                println("--------------------------")
            }
        }

        sc.stop
    }
}
