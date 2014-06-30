package rdf2neo

import java.util.zip.GZIPInputStream
import java.io.{BufferedInputStream, BufferedReader, InputStreamReader, FileInputStream}

class NTripleStream(filename:String) {
  val reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new BufferedInputStream(new FileInputStream(filename)))))

  def stream:Stream[(String,String,String)] =
    Stream.continually(reader.readLine)
      .takeWhile((s:String) => s != null)
      .map(parseTriple(_))

  def parseTriple(triple:String):(String,String,String) = {
    val Array(subj:String, pred:String, obj:String, _:String) = triple.split("\t")
    return (subj,pred,obj)
  }

}