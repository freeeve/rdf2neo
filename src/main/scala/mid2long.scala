package rdf2neo

object mid2long {
 
  val mask = Integer.parseInt("11111", 2)
  val bits = 5
  val maxChars = 8 / bits
  var enc = Array.fill[Int](128)(-1)
  var dec = Array.fill[Char](32)('_')
  
  var code = 0
  ('0' to '9').foreach{c => enc(c)=code; dec(code)=c; code+=1}
  ('a' to 'z').foreach{c => if(!vowel(c)) {enc(c)=code; dec(code)=c; code+=1}}
  //('Z' to 'Z').foreach{c => if(!vowel(c)) {enc(c)=code; dec(code)=c; code+=1}}
  enc('_')=code
  dec(code)='_'

  def vowel(c:Char) = 
    (c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u')

  // 10s for 100M encodes... probably not worth optimizing
  def encode(mid:String):Long = {
    var result = 0L
    var length = mid.length
    (0 until length).foreach{i =>
      if (mid.charAt(i) >= 128) {
        return -1
      }
      result |= enc(mid.charAt(i))
      if (i != length-1) result <<= bits
    }
    result
  }
 
  def decode(l:Long):String = {
    val result = Array.fill[Char](maxChars)(' ')
    var i=0
    var mid = l
    ((maxChars-1) to 0 by -1).foreach{i =>
      result(i) = dec((mid & mask).toInt)
      mid = mid >> bits
      if (mid == 0) {
        return String.valueOf(result,i,maxChars-i)
      }
    } 
    String.valueOf(result,i,maxChars-i)
  }
}
