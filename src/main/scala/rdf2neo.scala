package rdf2neo

import collection.JavaConverters._
import annotation.tailrec

import org.neo4j.graphdb.{DynamicRelationshipType, DynamicLabel}
import org.neo4j.unsafe.batchinsert.BatchInserters

import gnu.trove.map.hash.TObjectLongHashMap

import java.io.{BufferedReader, InputStreamReader, FileInputStream}
import java.util.zip.GZIPInputStream

object Main extends App {
  val inserter = BatchInserters.inserter(Settings.outputGraphPath);

  val is = new GZIPInputStream(new FileInputStream(Settings.gzippedTurtleFile))
  val in = new BufferedReader(new InputStreamReader(is))
  var count:Long = 0
  var instanceCount:Long = 0
  val startTime = System.currentTimeMillis
  val idMap = new TObjectLongHashMap[String]()
  Stream.continually(in.readLine()).takeWhile(_ != null).foreach(processTurtle(_))

  inserter.shutdown();

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
        val subjSplit = subj.split("\\.")
        // check if this is a node we want to keep
        if(Settings.nodeTypePredicates.contains(pred) 
       && (Settings.nodeTypeSubjects.isEmpty || listStartsWith(Settings.nodeTypeSubjects, subj))
//       && (Settings.nodeTypeSubjects.isEmpty || Settings.nodeTypeSubjects.contains(subj))
        ) {
          val objSplit = obj.split("\\.")
          println("setting label: "+turtle)
          if(!idMap.contains(objSplit(1))) {
            instanceCount += 1
            idMap.put(objSplit(1), new java.lang.Long(instanceCount)) 
            inserter.createNode(instanceCount, null)
          } 
          var curLabels = inserter.getNodeLabels(instanceCount).asScala.toArray
          curLabels = curLabels :+ DynamicLabel.label(subj)
          inserter.setNodeLabels(instanceCount, curLabels : _*) // the _* is for varargs
        } else if (subjSplit.length == 2 && idMap.contains(subjSplit(1))) { 
          val objSplit = obj.split("\\.")
          // if this is a property/relationship of a node
          if(objSplit.length == 2 && idMap.contains(objSplit(1))) {
            // this is a relationship!
            val subjId = idMap.get(subjSplit(1))
            val objId = idMap.get(objSplit(1))
            inserter.createRelationship(subjId, objId, DynamicRelationshipType.withName(pred), null)
          } else {
            // this is a real property
            println("setting property: " + turtle)
            val id = idMap.get(subjSplit(1))
            println("found id: " + id)
            if(inserter.nodeHasProperty(id, pred)) {
              println("already has prop: " + id + "; pred: "+pred)
              var prop = inserter.getNodeProperties(id).get(pred)
              prop = prop match {
                case prop:Array[Any] => {println("prop array detected..."); prop + obj}
                case _ => {println("converting prop to array..."); Array(prop) + obj}
              }
              inserter.setNodeProperty(id, pred, prop)
            } else {
              inserter.setNodeProperty(id, pred, obj) 
            }
          }
        } else {
          //println("doesn't match filters: " + turtle)
        }
      } else {
        println("Line split with non-triple: " + turtle)
      }
    }
  }

  def sanitize(str:String):String = {
    str.replaceAll("[^A-Za-z0-9]", "")
  }

  @inline def listStartsWith(list:Seq[String], str:String):Boolean = {
    @inline @tailrec def listStartsWith(list:Seq[String], str:String, i:Int):Boolean = {
      if(i >= list.length) {
        false
      } else if(list(i).startsWith(str)) {
        true
      } else {
        listStartsWith(list, str, i+1)
      }
    }

    listStartsWith(list, str, 0)
  }

/*
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
*/
}
