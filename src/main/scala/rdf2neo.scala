package rdf2neo

import org.neo4j.tooling._
import org.neo4j.kernel._
import org.neo4j.graphdb._
import org.neo4j.graphdb.schema._
import org.neo4j.graphdb.factory._
import org.neo4j.unsafe.batchinsert._
import collection.JavaConverters._
import gnu.trove.map.hash.TObjectLongHashMap

import java.io.{BufferedReader, InputStreamReader, FileInputStream}
import java.util.zip.GZIPInputStream

object Main extends App {
  //val inserter = BatchInserters.inserter(Settings.outputGraphPath);

  val is = new GZIPInputStream(new FileInputStream(Settings.gzippedTurtleFile))
  val in = new BufferedReader(new InputStreamReader(is))
  var count:Long = 0
  var instanceCount:Long = 0
  val startTime = System.currentTimeMillis
  val idMap = new TObjectLongHashMap[String]()
  Stream.continually(in.readLine()).takeWhile(_ != null).foreach(processTurtle(_))

  //inserter.shutdown();

  def processTurtle(turtle:String) = {
    count += 1
    if(count % 10000000 == 0) {
      println(count + " turtle lines processed; elapsed: " + ((System.currentTimeMillis - startTime) / 1000) + "s")
      println("instanceCount: " + instanceCount)
      println("idMap size: " + idMap.size)
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
        && pred.equals("ns:type.type.instance")
        //Settings.nodeTypePredicates.contains(pred) 
        // &&  Settings.nodeTypePredicateFilter.contains(obj)
           // more filters here
         ) {
            //println("subj: "+subj)
            //println("pred: "+pred)
            //println("obj: "+obj)
            val arr = obj.split("\\.")
            instanceCount += 1
            if(!idMap.contains(arr(1))) {
              idMap.put(arr(1), instanceCount) 
            } else {
              println("duplicate: " + turtle)
            }
            //val s = inserter.createNode(Map[String,Object]("value" -> sanitize(subj)).asJava, label("Subject"))
            //val o = inserter.createNode(Map[String,Object]("value" -> sanitize(obj)).asJava, label("Object"))
            //val p = inserter.createRelationship(s, o, relType(sanitize(pred)), null)
           
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
