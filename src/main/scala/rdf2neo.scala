package rdf2neo

import java.lang.RuntimeException
import collection.JavaConverters._
import org.neo4j.graphdb.{DynamicRelationshipType, DynamicLabel}
import org.neo4j.unsafe.batchinsert.BatchInserters
import java.io._
import java.util.zip.GZIPInputStream
import mid2long.encode

abstract class RdfError(errorString: String)

case class MalformedRdfError(errorString: String) extends RdfError(errorString)

case class MissingIdRdfError(errorString: String) extends RdfError(errorString)

case class PropertyExistsRdfError(errorString: String) extends RdfError(errorString)

//match (n:freebase) return n

//https://groups.google.com/forum/#!msg/freebase-discuss/AG5sl7K5KBE/iR7p-YfTNsUJ
//-XX:CMSInitiatingOccupancyFraction=<percent>

object Main extends App {

  val inserter = BatchInserters.inserter(Settings.outputGraphPath);

  val errorLogFileWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(Settings.errorLogFile)))
  val statusLogFileWriter = if (Settings.statusLogFile.isEmpty) None else Some(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(Settings.statusLogFile))))

  val neoFlag = false

  val objectTrimmerRegex = """^\"|\"$""".r
  val sanitizeRegex = """[^A-Za-z0-9]""".r

  val ALL = -1L
  val ONE_MILLION = 1000000L
  val ONE_BILLION = 1000000000L

  val linesToProcess = ONE_MILLION * 100L

  var processName = "";

  var rdfLineCount = 0L
  var instanceCount = 0L
  val startTime = System.currentTimeMillis
  var lastTime = System.currentTimeMillis
  val idMap = new IdMap //new TObjectLongHashMap[String]()

  //println("Settings.nodeTypeSubjects:" + Settings.nodeTypeSubjects)
  //println("Settings.nodeTypePredicates:" + Settings.nodeTypePredicates)

  try {
    processRdfFile(processRdfLineForIds)
    idMap.done
    processRdfFile(processRdfLineBuildRelationships)
  }
  finally {
    inserter.shutdown();
    errorLogFileWriter.close

    statusLogFileWriter match {
      case Some(fileWriter) => fileWriter.close
      case _ => {}
    }
  }

  def writeToStatusLog(string: String) = {

    statusLogFileWriter match {
      case Some(fileWriter) => fileWriter.write(string + "\r\n")
      case _ => {}
    }
  }

  def parseTriple(rdfLine: String) = {

    val idx = rdfLine.indexOf('\t')

    if (idx <= 0)
      (trim(rdfLine.substring(0, idx)))

    val first = trim(rdfLine.substring(0, idx))

    val idx2 = rdfLine.indexOf('\t', idx + 1)

    if (idx2 <= 0)
      (first,
        trim(rdfLine.substring(idx + 1, idx2)))

    val second = trim(rdfLine.substring(idx + 1, idx2))

    val idx3 = rdfLine.lastIndexOf('\t')

    val third = trim(rdfLine.substring(idx2 + 1, idx3))

    // not in English || is a key, return 0 so we don't process it

    if (third.isEmpty || second.isEmpty)
      ()
    else
      (first, second, third)
  }

  def processSubjectSetRdfLine(subject: String, predicate: String, obj: String, tripleString: String) = {

    if (subject.contains("math") || (subject.contains("computer") && subject.contains("science"))) writeToStatusLog(tripleString + "\r\n")
  }

  def printStatus() = {

    if ((rdfLineCount != ALL) && (rdfLineCount >= linesToProcess)) {

      throw new RuntimeException
    }

    rdfLineCount += 1

    if (rdfLineCount % 10000000 == 0) {

      val curTime = System.currentTimeMillis

      println(rdfLineCount / 10000000 + "0M tripleString lines processed(" + "); elapsed: " +
        ((curTime - startTime) / 1000) + "s; last 10M: " + ((curTime - lastTime) / 1000) + "s")

      lastTime = curTime
      //println("idMap size: " + idMap.size)
    }

  }

  def logRdfError(rdfError: RdfError, tripleString: String, rdfTuple: Any) = {

    rdfError match {

      case MalformedRdfError(errorString) => {

        rdfTuple match {
          case (first: String, second: String, third: String) => {
            errorLogFileWriter.write(" tupple size : 3 <" + first + "> <" + second + "> <" + third + "> line:\r\n " + tripleString + "\r\n")
          }
          case (first: String, second: String) => {
            errorLogFileWriter.write(" tupple size : 2 <" + first + "> <" + second + "> line:\r\n " + tripleString + "\r\n")
          }
          case (first: String) => {
            errorLogFileWriter.write(" tupple size : 1 <" + first + "> line:\r\n " + tripleString + "\r\n")
          }
          case () => {
            errorLogFileWriter.write(" empty tuple " + " line:\r\n " + tripleString + "\r\n")
          }
          case _ => {
            errorLogFileWriter.write(" tupple type : " + rdfTuple.getClass.getName + " line:\r\n " + tripleString + "\r\n")
          }
        }
      }

      case MissingIdRdfError(errorString) => {
      }

      case PropertyExistsRdfError(errorString) => {
      }

      case _ => {
      }

    }
  }

  def stringContainsAllStrings(string: String, stringList: Seq[String]): Boolean = {

    def stringContainsAllStringsR(string: String, stringList: Seq[String]): Boolean = {

      if (stringList.isEmpty) {
        true
      } else if (!string.contains(stringList.head)) {
        false
      } else {
        stringContainsAllStringsR(string, stringList.tail)
      }
    }

    stringContainsAllStringsR(string, stringList)
  }

  def stringContainsStringConjunctions(string: String, conjunctionList: Seq[Seq[String]]): Boolean = {

    def stringContainsStringConjunctionsR(string: String, conjunctionList: Seq[Seq[String]]): Boolean = {

      if (conjunctionList.isEmpty) {
        false
      } else if (stringContainsAllStrings(string, conjunctionList.head)) {
        true
      } else {
        stringContainsStringConjunctionsR(string, conjunctionList.tail)
      }
    }

    stringContainsStringConjunctionsR(string, conjunctionList)
  }

  def stringContainsStrings(string: String, stringList: Seq[String]): Boolean = {

    def stringContainsStringsR(string: String, stringList: Seq[String]): Boolean = {

      if (stringList.isEmpty) {
        false
      } else if (string.contains(stringList.head)) {
        //println("found:" + stringList.head)
        true
      } else {
        stringContainsStringsR(string, stringList.tail)
      }
    }
    //println("checking:" + stringList.head)
    stringContainsStringsR(string, stringList)
  }

  def isValidTriple(subject: String, predicate: String, obj: String) = {

    if (subject.isEmpty || predicate.isEmpty || obj.isEmpty)
      false

    if (subject.startsWith("@base") || subject.startsWith("@prefix") || subject.startsWith("#"))
      false

    true
  }

  def isValidIdTriple(subject: String, predicate: String, obj: String) = {
    //println("predicate : " + predicate)
    //println("Settings.nodeTypePredicates.contains(predicate) : " + Settings.nodeTypePredicates.contains(predicate))
    if (isValidTriple(subject, predicate, obj) &&
      Settings.nodeTypePredicates.contains(predicate) &&
      ((Settings.nodeTypeSubjects.isEmpty || stringContainsStrings(subject, Settings.nodeTypeSubjects)) ||
        (Settings.nodeTypeSubjectsConjunctive.isEmpty || stringContainsStringConjunctions(subject, Settings.nodeTypeSubjectsConjunctive))))
      true
    else
      false
  }


  def sanitize(string: String) = {
    sanitizeRegex.replaceAllIn(string, "_")
  }

  def processRdfLineForIds(subject: String, predicate: String, obj: String) = {

    //writeToStatusLog("processRdfLineForIdssetting with line: " + tripleString)
    //writeToStatusLog("isValidTriple,isValidIdTriple: " + isValidTriple(subject, predicate, obj) + ", " + isValidIdTriple(subject, predicate, obj))

    if (isValidIdTriple(subject, predicate, obj)) {

      if (!idMap.contains(encode(obj))) {
        writeToStatusLog("adding id: " + List(subject,predicate,obj).mkString("\n"))

        instanceCount += 1
        idMap.put(encode(obj))

        if (neoFlag) {
          inserter.createNode(instanceCount, Map[String, Object]("mid" -> obj).asJava, DynamicLabel.label("freebase"))
        }
      }

      if (neoFlag) {

        var curLabels = inserter.getNodeLabels(instanceCount).asScala.toArray

        curLabels = curLabels :+ DynamicLabel.label(sanitize(subject))

        inserter.setNodeLabels(instanceCount, curLabels: _*) // the _* is for varargs
      }
    }
  }

  def processRdfLineBuildRelationships(subject: String, predicate: String, obj: String) = {

    if (isValidTriple(subject, predicate, obj)) {

      if (idMap.contains(encode(subject))) {
        // this is a property/relationship of a node
        val subjectId = idMap.get(encode(subject))
        val sanitizedPredicate = sanitize(predicate)

        if (idMap.contains(encode(obj))) {
          // this is a relationship
          writeToStatusLog("creating relationship: " + List(subject,predicate,obj).mkString("\n"))
          val objId = idMap.get(encode(obj))

          if (neoFlag) {

            inserter.createRelationship(subjectId, objId, DynamicRelationshipType.withName(sanitizedPredicate), null)
          }

        } else {
          // this is a real property
          writeToStatusLog("setting property: "  + List(subject,predicate,obj).mkString("\n"))

          if (obj.startsWith("<http://rdf.freebase.com/ns/m.")) {
            logRdfError(MissingIdRdfError("dropping relationship on the ground for an id we don't have:"), List(subject,predicate,obj).mkString("\n"), (subject, predicate, obj))

          } else {

            val trimmedObj = objectTrimmerRegex.replaceAllIn(obj, "")

            if ((trimmedObj.length > 3 && trimmedObj.substring(trimmedObj.length - 3)(0) != '.' || trimmedObj.endsWith(".en"))
              && (sanitizedPredicate.length > 3 && sanitizedPredicate.substring(sanitizedPredicate.length - 3)(0) != '_' || sanitizedPredicate.endsWith("_en"))) {

              if (neoFlag) {

                if (inserter.nodeHasProperty(subjectId, sanitizedPredicate)) {
                  logRdfError(PropertyExistsRdfError("already has prop: " + subjectId + "; predicate: " + predicate), List(subject,predicate,obj).mkString("\n"), (subject, predicate, obj))

                  var prop = inserter.getNodeProperties(subjectId).get(sanitizedPredicate)
                  inserter.removeNodeProperty(subjectId, sanitizedPredicate)
                  writeToStatusLog("got node property: " + subjectId + ":" + predicate + "; prop: " + prop)
                  prop match {
                    case prop: Array[String] => {
                      //writeToStatusLog("prop array detected...");
                      inserter.setNodeProperty(subjectId, sanitizedPredicate, prop :+ trimmedObj)
                    }
                    case _ => {
                      //writeToStatusLog("converting prop to array...");
                      inserter.setNodeProperty(subjectId, sanitizedPredicate, Array[String](prop.toString) :+ trimmedObj)
                    }
                  }
                } else {
                  inserter.setNodeProperty(subjectId, sanitizedPredicate, trimmedObj)
                }
              }

            }
          }
        }
      }
    }
  }

  def processRdfFile(rdfLineProcessor: (String, String, String) => Unit) = {
    val rdfstream = new NTripleStream(Settings.gzippedNTripleFile)
    rdfLineCount = 0

    rdfstream.stream.
      foreach {triple =>
        printStatus
        triple match {
          case (subject: String, predicate: String, obj: String) => rdfLineProcessor(subject, predicate, obj)
          case _ => {} //logRdfError(MalformedRdfError(""), tripleString, rdfTriple)
        }
      }
  }

  def trim(string: String): String = {
    val length = string.length()

    if (string.startsWith("\"") && string.contains("\"@")) {
      if (string.endsWith("\"@en")) {
        string.substring(1, length - 4)
      }
      else {
        ""
      } // only care about English for now
    } else if (string.contains("/key/")) {
      // keys are not human readable and we throw it away
      ""
    } else if (string.startsWith(Settings.fbRdfPrefix) && string.endsWith(">")) {
      // <http://rdf.freebase.com/ns/g.1254x65_q>
      string.substring(Settings.fbRdfPrefixLen, length - 1)
    } else if (string.startsWith("\"") && string.endsWith("\"")) {
      string.substring(1, length - 1)
    } else if (string.startsWith("<") && string.endsWith(">")) {
      string.substring(1, length - 1)
    } else if (string.startsWith("\"") && string.contains("\"^^")) {
      // "1987-06-17"^^<http://www.w3.org/2001/XMLSchema#date>
      string.substring(1, string.indexOf("\"^^"))
    } else {
      string
    }
  }
}
