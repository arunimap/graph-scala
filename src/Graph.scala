import scala.collection.mutable.ListBuffer

/**
  * Simple graph implementation built off of `Redis` key-value store.
  */
class Graph {

  private var r: Redis = new Redis()

  /**
    * Add the given node to the graph. Node name may not contain '_edges' or be called 'nodes'.
    * Flexible so could store other information if needed other than node name if needed.
    *
    * @param v the node to be added
    */
  def addNode(v: String): Unit = {
    r.set(v, v) // set key w/ node name
    r.lpush("nodes", v) // add node to list of all nodes
  }

  /** Add an edge connecting the two given edges.
    *
    * @param u the start edge
    * @param v the end edge
    */
  def addEdge(u: String, v: String): Unit = {
    if (r.get(u) == null | r.get(v) == null) {
      println("One of the specified nodes does not exist.")
    } else {
      r.rpush(u + "_edges", v)
      r.rpush(v + "_edges", u)
    }
  }

  /** Find all nodes directly connected to the given node.
    *
    * @param v the node we are looking for neighbors of
    * @return a list of nodes adjacent to this node (connected w/ edges)
    */
  def adjacent(v: String): List[String] = {
    r.lrange(v + "_edges", 0, r.llen(v + "_edges"))
  }

  /**
    *
    * @param u
    * @param v
    * @return
    */
  def shortestPath(u: String, v: String): List[String] = {
    if (r.get(u) == null | r.get(v) == null) {
      println("One of the specified nodes does not exist.")
      ListBuffer[String]().toList
    } else {
      djikstra(u, v)
    }
  }

  /**
    * Implementation of Djiktra's algorithm, to find the shortest path from source to target in
    * this graph. Assumes both source and target exist in this graph, as will only be called via
    * `shortestPath` method above.
    *
    * @param source the starting node
    * @param target the node we are trying to get to
    * @return The shortest path from the source node to the target
    */
  private def djikstra(source: String, target: String): List[String] = {
    // all nodes
    val nodes: List[String] = r.lrange("nodes", 0, r.llen("nodes"))

    // Accumulators
    var dist = scala.collection.mutable.Map[String, Double]() // distances from source
    var prev = scala.collection.mutable.Map[String, String]() // previous node
    nodes.foreach(x => dist += (x -> Double.PositiveInfinity))
    nodes.foreach(x => prev += (x -> null))
    dist = dist += source -> 0 // set start to 0

    // keep track of nodes which have not been visited
    var toVisit: ListBuffer[String] = ListBuffer[String]()
    nodes.foreach(x => toVisit.+=(x)) // add all nodes to be visited

    // accumulator to return
    var seq: ListBuffer[String] = ListBuffer[String]()

    while (toVisit.nonEmpty) {
      val unvisitedDist = dist.filterKeys(x => toVisit.contains(x))
      val minVertex: String = unvisitedDist.filter(x => x._2 == unvisitedDist.values.min).head._1

      toVisit = toVisit.-=(minVertex) // remove the node currently being visited

      if (!toVisit.contains(target)) {
        var u = target
        if (prev.get(u) != null) {
          while (u != null) {
            seq = seq.+:(u)
            u = prev.getOrElse(u, null)
          }
          toVisit.clear()
        } else {
          println("No path exists between the specified nodes.")
          return List[String]();
        }
      } else {
        // for each neighbor of the minimum vertex
        adjacent(minVertex).foreach(
          v => // if less than the existing value, replace, and update accumulators
            if (dist.getOrElse(v, Double.PositiveInfinity).isInfinity |
              dist.getOrElse(v, Double.PositiveInfinity) > dist(minVertex) + 1.0) {
              dist += v -> (dist(minVertex) + 1.0)
              prev += v -> minVertex
            })
      }
    }
    // finally return the accumulator
    seq.toList
  }

}

/**
  * Test object for `Graph` class, and its methods.
  */
object TestGraph extends App {

  /**
    * Initialize the example graph from the assignment.
    */
  val g = new Graph()
  def init(): Unit = {
    // add nodes
    g.addNode("X")
    g.addNode("J")
    g.addNode("B")
    g.addNode("F")
    g.addNode("R")
    g.addNode("C")
    g.addNode("E")
    g.addNode("Y")

    // add edges
    g.addEdge("X", "J")
    g.addEdge("J", "B")
    g.addEdge("J", "F")
    g.addEdge("J", "R")
    g.addEdge("B", "F")
    g.addEdge("B", "R")
    g.addEdge("B", "C")
    g.addEdge("F", "E")
    g.addEdge("R", "Y")
    g.addEdge("R", "E")
    g.addEdge("R", "C")
    g.addEdge("E", "Y")
  }

  this.init()
  println("Path X to Y: " + g.shortestPath("X", "Y"))
//  println("Path X to X: " + g.shortestPath("X", "Z"))
}
