package rdf2neo

import org.neo4j.tooling._
import org.neo4j.kernel._
import org.neo4j.graphdb._
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import collection.JavaConverters._

object Main extends App {
  val graph = new GraphDatabaseFactory()
    .newEmbeddedDatabase(Settings.outputGraphPath)

  import java.io.{BufferedReader, InputStreamReader, FileInputStream}
  import java.util.zip.GZIPInputStream
  val is = new GZIPInputStream(new FileInputStream(Settings.gzippedTurtleFile))
  val in = new BufferedReader(new InputStreamReader(is))
  var count:Long = 0
  val startTime = System.currentTimeMillis
  Stream.continually(in.readLine()).takeWhile(_ != null).foreach(processTurtle(_))
  graph.shutdown

  def processTurtle(turtle:String) = {
    count += 1
    if(count % 10000000 == 0) {
      println(count + " turtle lines processed; elapsed: " + ((System.currentTimeMillis - startTime) / 1000) + "s")
    }
    if(turtle.startsWith("@base")) {
      // do we need to handle these? 
    } else if (turtle.startsWith("@prefix")) {
      // do we need to handle these?
    } else if (turtle.startsWith("#")) { 
      // definitely don't need to handle these
    } else if (turtle.length > 1) {
      val arr = turtle.substring(0,turtle.length-1).split("\\t")
      if(arr.length == 3) {
        val (subj, pred, obj) = (arr(0), arr(1), arr(2))
        if(Settings.nodeTypePredicates.contains(pred) && 
           Settings.nodeTypePredicateFilter.contains(obj)
           // more filters here
         ) {
          val tx = graph.beginTx
          try {
            // http://docs.neo4j.org/chunked/milestone/tutorials-java-embedded-new-index.html
            println("subj: "+subj)
            println("pred: "+pred)
            println("obj: "+obj)
            tx.success
          } finally {
            tx.finish
          }
        }
      } else {
        println("Line split with non-triple: " + turtle)
      }
    }
  }

  def relType(str:String):RelationshipType = DynamicRelationshipType.withName(str)

  def label(str:String):Label = DynamicLabel.label(str)
}
