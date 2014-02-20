package rdf2neo

import collection.JavaConverters._
import com.typesafe.config._

import java.util.ArrayList

object Settings {
  val config = ConfigFactory.load(
                 "rdf2neo",
                 ConfigParseOptions.defaults()
                   .setSyntax(ConfigSyntax.JSON)
                   .setAllowMissing(false),
                 ConfigResolveOptions.defaults()
                   .setUseSystemEnvironment(false))
  //println("DEBUG settings:")
  //println(config.root().render())
  //config.checkValid(ConfigFactory.defaultReference(), "rdf2neo")

  val fbRdfPrefix = config.getString("fbRdfPrefix")
  val fbRdfPrefixLen = fbRdfPrefix.length()

  val outputGraphPath = config.getString("outputGraphPath")
  val gzippedTurtleFile = config.getString("gzippedTurtleFile")
  val errorLogFile = config.getString("errorLogFile")
  val statusLogFile = config.getString("statusLogFile")
  val nodeTypeSubjects = config.getList("nodeTypeSubjects").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
  val nodeTypeSubjectsConjunctive = config.getList("nodeTypeSubjectsConjunctive").unwrapped.asScala.toSeq.map(_.asInstanceOf[ArrayList[String]].asScala.toSeq)

  val nodeTypePredicates = config.getList("nodeTypePredicates").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
}
