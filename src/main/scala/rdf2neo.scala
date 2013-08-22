package rdf2neo

import org.neo4j.tooling._
import org.neo4j.kernel._
import org.neo4j.graphdb._
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import collection.JavaConverters._

object Main extends App {
  val graph = new GraphDatabaseFactory()
    .newEmbeddedDatabase(Settings.outputGraphPath)
  val rootzip = new java.util.zip.ZipFile(Settings.zippedTurtleFile)
  val entries = rootzip.entries.asScala
  entries.foreach { e =>
    import java.io.{BufferedReader, InputStreamReader}
    val is = rootzip.getInputStream(e)
    val in = new BufferedReader(new InputStreamReader(is))
    Stream.continually(in.readLine()).takeWhile(_ != null).foreach(processTurtle(_))
  }
  graph.shutdown

  def processTurtle(turtle:String) = {
    if(turtle.startsWith("@base")) {
      // do we need to handle these? 
    } else if (turtle.startsWith("@prefix")) {
      // do we need to handle these?
    } else if (turtle.startsWith("#")) { 
      // definitely don't need to handle these
    } else {
      val arr = turtle.substring(0,turtle.length-1).split("\\s+")
      if(arr.length == 3) {
        val (subj, pred, obj) = (arr(0), arr(1), arr(2))
        if(Settings.nodeTypePredicates.contains(pred) && 
           Settings.nodeTypePredicateFilter.contains(obj)
           // more filters here
         ) {
          val tx = graph.beginTx
          try {
            // http://docs.neo4j.org/chunked/milestone/tutorials-java-embedded-new-index.html
            println("subj: "+subj);
            println("pred: "+pred);
            println("obj: "+obj);
            tx.success
          } finally {
            tx.finish
          }
        }
      }
    }
  }

  def relType(str:String):RelationshipType = DynamicRelationshipType.withName(str)

  def label(str:String):Label = DynamicLabel.label(str)
}
