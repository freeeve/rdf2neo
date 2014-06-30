
class idMapSpec extends FlatSpec with ShouldMatchers {

  val idMap = rdf2neo.IdMap()

  "idMap" should "be able to add new ids" in {
    val seq = Seq[String]("abc", "bcd")
    Main.listStartsWith(seq, "cde") should be(false)
    Main.listStartsWith(seq, "a") should be(false)
    Main.listStartsWith(seq, "abc") should be(true)
    Main.listStartsWith(seq, "bc") should be(false)
    Main.listStartsWith(seq, "") should be(false)
  }

  "Main.fastSplit" should "be able to split a string into an array" in {
    var str = "hello\tthis\tis a string"
    val arr = Array[String]("","","")
    Main.fastSplit(arr, str) should equal(3)
    arr should equal(Array[String]("hello","this","is a string"))
  }

}
