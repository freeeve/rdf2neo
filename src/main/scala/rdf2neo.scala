package rdf2neo

import org.neo4j.tooling._
import org.neo4j.kernel._
import org.neo4j.graphdb._
import org.neo4j.graphdb.schema._
import org.neo4j.graphdb.factory._
import collection.JavaConverters._

object Main extends App {
  val graph = new GraphDatabaseFactory()
    .newEmbeddedDatabaseBuilder(Settings.outputGraphPath)
    .setConfig(GraphDatabaseSettings.cache_type, "none" )
    .setConfig(GraphDatabaseSettings.keep_logical_logs, "false" )
    .newGraphDatabase();
//  configureIndex(graph, label("Subject"), "value")
//  configureIndex(graph, label("Object"), "value")

  import java.io.{BufferedReader, InputStreamReader, FileInputStream}
  import java.util.zip.GZIPInputStream
  val is = new GZIPInputStream(new FileInputStream(Settings.gzippedTurtleFile))
  val in = new BufferedReader(new InputStreamReader(is))
  var count:Long = 0
  val startTime = System.currentTimeMillis
  var tx = graph.beginTx
  try {
    Stream.continually(in.readLine()).takeWhile(_ != null).foreach(processTurtle(_))
    tx.success
  } finally {
    tx.finish
  }
  graph.shutdown

  def processTurtle(turtle:String) = {
    count += 1
    if(count % 100000 == 0) {
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
        if(true
        //Settings.nodeTypePredicates.contains(pred) 
        // &&  Settings.nodeTypePredicateFilter.contains(obj)
           // more filters here
         ) {
            // http://docs.neo4j.org/chunked/milestone/tutorials-java-embedded-new-index.html
            //println("subj: "+subj)
            //println("pred: "+pred)
            //println("obj: "+obj)
            val s = graph.createNode(label("Subject"))
            val o = graph.createNode(label("Object"))
            val p = s.createRelationshipTo(o, relType(sanitize(pred)))
            s.setProperty("value", sanitize(subj))
            p.setProperty("value", sanitize(obj))

        } else {
          //println("doesn't match filters: " + turtle)
        }
      } else {
        println("Line split with non-triple: " + turtle)
      }
    }
  }

  def relType(str:String):RelationshipType = DynamicRelationshipType.withName(str)

  def label(str:String):Label = DynamicLabel.label(str)

  def sanitize(str:String):String = {
    str.replaceAll("[^A-Za-z0-9]", "")
  }

  def configureIndex(graphDb:GraphDatabaseService, l:Label, key:String):IndexDefinition = {
    var indexDefinition:IndexDefinition = null
    val tx = graphDb.beginTx();
    try {
      val indexDefinition = graphDb.schema.indexFor(l)
        .on(key)
        .create()
      tx.success()
    } finally {
      tx.finish()
    }
    indexDefinition
  }
}
