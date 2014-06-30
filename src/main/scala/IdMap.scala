package rdf2neo

import java.util

class IdMap {
  def apply:IdMap = new IdMap()
  var arr = Array.fill[Long](100000000)(Long.MaxValue)
  var idx = 0
  var flag = false
  def put(mid:Long) = {
    arr(idx) = mid
    idx += 1
  }
  def get(mid:Long):Long = {
    if(!flag) throw new Exception("need to call done() first.")
    util.Arrays.binarySearch(arr, mid, 0, idx).toLong
  }
  def contains(mid:Long):Boolean = {
    get(mid) > 0
  }
  def done = {
    util.Arrays.sort(arr)
    flag = true
  }
}