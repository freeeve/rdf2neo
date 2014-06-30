package rdf2neo

import java.util

class IdMap {
  def apply:IdMap = new IdMap()
  var arr = Array.fill[Long](100000000)(-1)
  var idx = 0
  def put(mid:Long) = {
    arr(idx) = mid
    idx += 1
  }
  def get(mid:Long):Long = {
    util.Arrays.binarySearch(arr, mid).toLong
  }
  def contains(mid:Long):Boolean = {
    get(mid) > 0
  }
  def done = {
    util.Arrays.sort(arr)
  }
}