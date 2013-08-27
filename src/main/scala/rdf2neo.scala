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

  var is = new GZIPInputStream(new FileInputStream(Settings.gzippedTurtleFile))
  var in = new BufferedReader(new InputStreamReader(is))
  var count:Long = 0
  var instanceCount:Long = 0
  val startTime = System.currentTimeMillis
  var lastTime = System.currentTimeMillis
  val turtleSplit = Array[String]("","","") // reused to avoid GC
  val idMap = new TObjectLongHashMap[String]()

  Stream.continually(in.readLine)
        .takeWhile(_ != null)
        .foreach(processTurtle(_, true))

  in.close
  is = new GZIPInputStream(new FileInputStream(Settings.gzippedTurtleFile))
  in = new BufferedReader(new InputStreamReader(is))

  count = 0
  Stream.continually(in.readLine)
        .takeWhile(_ != null)
        .foreach(processTurtle(_, false))

  inserter.shutdown();

  @inline def fastSplit(arr:Array[String], turtle:String):Int = {
    var c = 0
    var idx = turtle.indexOf('\t')
    if(idx > 0) c += 1
    else return c
    arr(0) = turtle.substring(0, idx)
    var idx2 = turtle.indexOf('\t', idx+1)
    if(idx2 > 0) c += 1
    else return c
    arr(1) = turtle.substring(idx+1, idx2)
    arr(2) = turtle.substring(idx2+1, turtle.length-1)
    return c+1
  }

  @inline def processTurtle(turtle:String, idOnly:Boolean) = {
    count += 1
    if(count % 10000000 == 0) {
      val curTime = System.currentTimeMillis
      println(count/1000000 + 
        "M turtle lines processed(" +
        (if(idOnly) "first pass" else "second pass") +
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
      val arrlength = fastSplit(turtleSplit, turtle)
      if(arrlength == 3) {
        val (subj, pred, obj) = (turtleSplit(0), turtleSplit(1), turtleSplit(2))
        // check if this is a node we want to keep
        if(idOnly == true && Settings.nodeTypePredicates.contains(pred) 
        && (Settings.nodeTypeSubjects.isEmpty || listStartsWith(Settings.nodeTypeSubjects, subj))
        ) {
          println("setting label: "+turtle)
          if(!idMap.contains(obj)) {
            instanceCount += 1
            idMap.put(obj, new java.lang.Long(instanceCount)) 
            inserter.createNode(instanceCount, Map[String,Object]("mid"->obj).asJava)
          } 
          var curLabels = inserter.getNodeLabels(instanceCount).asScala.toArray
          curLabels = curLabels :+ DynamicLabel.label(sanitize(subj))
          inserter.setNodeLabels(instanceCount, curLabels : _*) // the _* is for varargs
        } else if (idOnly == false && idMap.contains(subj)) { 
          // this is a property/relationship of a node
          val subjId = idMap.get(subj)
          val sanitizedPred = sanitize(pred)
          if(idMap.contains(obj)) {
            // this is a relationship
            println("creating relationship: " + turtle)
            val objId = idMap.get(obj)
            inserter.createRelationship(subjId, objId, DynamicRelationshipType.withName(sanitizedPred), null)
          } else {
            // this is a real property
            //println("setting property: " + turtle)
            if(obj.startsWith("ns:m.")) {
              //println("dropping relationship on the ground for an id we don't have: "+turtle)
            } else {
              val trimmedObj = obj.replaceAll("^\"|\"$", "")
              if((trimmedObj.length > 3 && trimmedObj.substring(trimmedObj.length-3)(0) != '.' || trimmedObj.endsWith(".en"))
              && (sanitizedPred.length > 3 && sanitizedPred.substring(sanitizedPred.length-3)(0) != '_' || sanitizedPred.endsWith("_en"))) { 
                if(inserter.nodeHasProperty(subjId, sanitizedPred)) {
                  //println("already has prop: " + subjId + "; pred: "+pred)
                  var prop = inserter.getNodeProperties(subjId).get(sanitizedPred)
                  inserter.removeNodeProperty(subjId, sanitizedPred)
                  //println("got node property: " +subjId + ":"+pred + "; prop: "+prop)
                  prop match {
                    case prop:Array[String] => {
                     // println("prop array detected..."); 
                      inserter.setNodeProperty(subjId, sanitizedPred, prop :+ trimmedObj)
                    }
                    case _ => {
                      //println("converting prop to array..."); 
                      inserter.setNodeProperty(subjId, sanitizedPred, Array[String](prop.toString) :+ trimmedObj)
                    }
                  }
                } else {
                  inserter.setNodeProperty(subjId, sanitizedPred, trimmedObj) 
                }
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
