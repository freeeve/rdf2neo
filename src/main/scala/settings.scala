package rdf2neo

import com.typesafe.config._

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

  val outputGraphPath = config.getString("outputGraphPath")
  val gzippedTurtleFile = config.getString("gzippedTurtleFile")
  val nodeTypePredicates = config.getList("nodeTypePredicates")
  val nodeTypePredicateFilter = config.getList("nodeTypePredicateFilter")
}
