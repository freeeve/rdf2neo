package rdf2neo

import org.neo4j.tooling._
import org.neo4j.kernel._
import org.neo4j.graphdb._
import collection.JavaConverters._

object Main extends App {
  val graph = new EmbeddedGraphDatabase(Settings.outputGraphPath)
  val rootzip = new java.util.zip.ZipFile(Settings.zippedTurtleFile)
  val entries = rootzip.entries.asScala
  entries.foreach { e =>
    import java.io.{BufferedReader, InputStreamReader}
    val is = rootzip.getInputStream(e)
    val in = new BufferedReader(new InputStreamReader(is))
    Stream.continually(in.readLine()).takeWhile(_ != null).foreach(println(_))
  }

  def relType(str:String):RelationshipType = DynamicRelationshipType.withName(str)

  def label(str:String):Label = DynamicLabel.label(str)
}
