name := "rdf2neo"
 
version := "0.1.0"
 
organization := "org.anormcypher"

scalaVersion := "2.10.2"

scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-unchecked", "-feature")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "org.neo4j" % "neo4j" % "2.0.0-M04"
)

seq(lsSettings :_*)

(LsKeys.tags in LsKeys.lsync) := Seq("rdf","neo4j", "neo")

(description in LsKeys.lsync) :=
  "Convert RDF to neo4j."
