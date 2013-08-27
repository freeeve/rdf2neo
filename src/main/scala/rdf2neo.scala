package rdf2neo

import collection.JavaConverters._
import annotation.tailrec

import java.io.{BufferedReader, InputStreamReader, FileInputStream}
import java.util.zip.GZIPInputStream

import org.neo4j.graphdb.{DynamicRelationshipType, DynamicLabel}
import org.neo4j.unsafe.batchinsert.BatchInserters

import gnu.trove.map.hash.TObjectLongHashMap

object Main extends App {
  val inserter = BatchInserters.inserter(Settings.outputGraphPath);

  val is = new GZIPInputStream(new FileInputStream(Settings.gzippedTurtleFile))
  val in = new BufferedReader(new InputStreamReader(is))
  var count:Long = 0
  var instanceCount:Long = 0
  val startTime = System.currentTimeMillis
  var lastTime = System.currentTimeMillis
  val idMap = new TObjectLongHashMap[String]()
  Stream.continually(in.readLine)
        .takeWhile(_ != null)
        .foreach(processTurtle(_, true))
  in.reset
  count = 0;
  Stream.continually(in.readLine)
        .takeWhile(_ != null)
        .foreach(processTurtle(_, false))

  inserter.shutdown();

  @inline def processTurtle(turtle:String, idOnly:Boolean) = {
    count += 1
    if(count % 10000000 == 0) {
      val curTime = System.currentTimeMillis
      println(count/1000000 + 
        "M turtle lines processed(" +
        if(idOnly) "first pass" else "second pass" +
        "); elapsed: " + 
        ((curTime - startTime) / 1000) + 
        "s; last 10M: " + 
        ((curTime - lastTime) / 1000) + 
        "s")
      lastTime = curTime
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
        // check if this is a node we want to keep
        if(idOnly == true && Settings.nodeTypePredicates.contains(pred) 
        && (Settings.nodeTypeSubjects.isEmpty || listStartsWith(Settings.nodeTypeSubjects, subj))
        ) {
          println("setting label: "+turtle)
          if(!idMap.contains(obj)) {
            instanceCount += 1
            idMap.put(obj, new java.lang.Long(instanceCount)) 
            inserter.createNode(instanceCount, null)
          } 
          var curLabels = inserter.getNodeLabels(instanceCount).asScala.toArray
          curLabels = curLabels :+ DynamicLabel.label(sanitize(subj))
          inserter.setNodeLabels(instanceCount, curLabels : _*) // the _* is for varargs
        } else if (idOnly == false && idMap.contains(subj)) { 
          // this is a property/relationship of a node
          val subjId = idMap.get(subj)
          if(idMap.contains(obj)) {
            // this is a relationship
            val objId = idMap.get(obj)
            inserter.createRelationship(subjId, objId, DynamicRelationshipType.withName(sanitize(pred)), null)
          } else {
            // this is a real property
            println("setting property: " + turtle)
            println("found id: " + subjId)
            if(obj.startsWith("ns:m.")) {
              println("dropping relationship on the ground for an id we don't have: "+turtle)
            } else {
              if(inserter.nodeHasProperty(subjId, pred)) {
                println("already has prop: " + subjId + "; pred: "+pred)
                var prop = inserter.getNodeProperties(subjId).get(pred)
                inserter.removeNodeProperty(subjId, pred)
                println("got node property: " +subjId + ":"+pred + "; prop: "+prop)
                prop match {
                  case prop:Array[String] => {
                    println("prop array detected..."); 
                    inserter.setNodeProperty(subjId, pred, prop :+ obj)
                  }
                  case _ => {
                    println("converting prop to array..."); 
                    inserter.setNodeProperty(subjId, pred, Array[String](prop.toString) :+ obj)
                  }
                }
              } else {
                inserter.setNodeProperty(subjId, pred, obj) 
              }
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
    str.replaceAll("[^A-Za-z0-9]", "_")
  }

  @inline def listStartsWith(list:Seq[String], str:String):Boolean = {
    @inline @tailrec def listStartsWith(list:Seq[String], str:String, i:Int):Boolean = {
      if(i >= list.length) {
        false
      } else if(str.startsWith(list(i))) {
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
