object Partition extends App {

  // Returns the fraction of records that would have to be re-assigned to a new node
  // if records were re-partitioned from startN to endN nodes
  // Note: records, startN, and endN must be positive
  def moved(records: Int, startN: Int, endN: Int): Double = {
    (1 to records).count(x => x % startN != x % endN) / records.toDouble
  }

  /* Test Cases */
  println("recs = 10, startN = 1, endN = 10, moved = " +
    moved(10, 1, 10))
  // Compute moved(1000000, 100, 107)
  println("recs = 1000000, startN = 100, endN = 107, moved = " +
    moved(1000000, 100, 107))
}
