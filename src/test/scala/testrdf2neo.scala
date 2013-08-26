package rdf2neo

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class rdf2neoSpec extends FlatSpec with ShouldMatchers {

  "Main.listStartsWith" should "be able check whether an item in the list starts with something" in {
    val seq = Seq[String]("abc", "bcd")
    Main.listStartsWith(seq, "cde") should be(false)
    Main.listStartsWith(seq, "a") should be(false)
    Main.listStartsWith(seq, "abc") should be(true)
    Main.listStartsWith(seq, "bc") should be(false)
    Main.listStartsWith(seq, "") should be(false)
  }

}
