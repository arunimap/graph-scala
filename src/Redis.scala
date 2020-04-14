import scala.collection.mutable.ListBuffer

/**
  * Basic implementation of redis-like key-value store, supporting basic string values and lists.
  */
class Redis() {

  // always initialize w/ an empty key-values stored
  private var key_val = new ListBuffer[(String, Either[String, ListBuffer[String]])]()

  /**
    * Get the value associated with the given key. Assumes valid state with no duplicate keys.
    *
    * @param key the key whose associated value we are looking for
    * @return the value associated with the given key, null if key does not exist, or throw an
    *         error if the value is a list-type
    */
  def get(key: String): String = {
    val m = key_val.indexWhere(x => x._1 == key) // find the index key-value pair if it exists

    m match {
      case -1 => null
      case _ => if (key_val(m)._2.isLeft) {
        key_val(m)._2.left.get.toString
      } else {
        throw new RuntimeException("WRONGTYPE: Operation against key holding wrong kind of value")
      }
    }
  }

  /**
    * Set the string value of a key
    *
    * @param key   the key we are setting the value for
    * @param value the string value we are either setting, or updating to for the given key
    */
  def set(key: String, value: String): Unit = {
    val m = key_val.indexWhere(x => x._1 == key) // find the index key-value pair if it exists

    m match {
      case -1 => key_val.+=((key, Left(value)))
      case _ => key_val.update(m, (key, Left(value)))
    }
  }

  /** Prepend the given value to the list specified by the given key.
    *
    * @param key   key whose value is to be altered
    * @param value the string value to be added to the start of the list value, throw error if
    *              key is already set to a string value
    */
  def lpush(key: String, value: String): Unit = {
    val m = key_val.indexWhere(x => x._1 == key) // find the index key-value pair if it exists

    m match {
      case -1 => key_val.+=((key, Right(new ListBuffer[String].:+(value))))
      case _ => if (key_val(m)._2.isLeft) {
        throw new RuntimeException("WRONGTYPE: Operation against key holding wrong kind of value")
      } else if (key_val(m)._2.isRight) {
        val rep: ListBuffer[String] = key_val(m)._2.right.get.+:(value)
        key_val.update(m, (key, Right(rep)))
      }
    }
  }

  /** Append the given value to the list specified by the given key.
    *
    * @param key   key whose value is to be altered
    * @param value the string value to be added to the end of the list value, throw error if the
    *              key is already set to a string value
    */
  def rpush(key: String, value: String): Unit = {
    val m = key_val.indexWhere(x => x._1 == key) // find the index key-value pair if it exists

    m match {
      case -1 => key_val.+=((key, Right(new ListBuffer[String].+=(value))))
      case _ => if (key_val(m)._2.isLeft) {
        throw new RuntimeException("WRONGTYPE: Operation against key holding wrong kind of value")
      } else if (key_val(m)._2.isRight) {
        val rep: ListBuffer[String] = key_val(m)._2.right.get.+=(value)
        key_val.update(m, (key, Right(rep)))
      }
    }
  }

  /** Remove and returns the first element of the list stored at the given key.
    *
    * @param key the key whose list-value we are altering
    * @return the element being removed, null if nothing removed, or throw error if the key maps
    *         to a string value
    */
  def lpop(key: String): String = {
    val m = key_val.indexWhere(x => x._1 == key) // find the index key-value pair if it exists

    m match {
      case -1 => null // key does not exist
      case _ =>
        if (key_val(m)._2.isLeft) { // cannot remove item from non-list value
          throw new RuntimeException("WRONGTYPE: Operation against key holding wrong kind of value")
        } else { // empty list has nothing to remove
          if (key_val(m)._2.right.get.isEmpty) {
            null
          } else { // actual removal from head case
            val s: String = key_val(m)._2.right.get.head
            key_val.update(m, (key, Right(key_val(m)._2.right.get.tail)))
            s
          }
        }
    }
  }

  /** Remove and returns the last element of the list stored at the given key.
    *
    * @param key the key whose list-value we are altering
    * @return the element being removed, null if nothing removed, or throw error if the key maps
    *         to a string value
    */
  def rpop(key: String): String = {
    val m = key_val.indexWhere(x => x._1 == key) // find the index key-value pair if it exists

    m match {
      case -1 => null // key does not exist
      case _ =>
        if (key_val(m)._2.isLeft) { // cannot remove item from non-list value
          throw new RuntimeException("WRONGTYPE: Operation against key holding wrong kind of value")
        } else { // empty list has nothing to remove
          if (key_val(m)._2.right.get.isEmpty) {
            null
          } else { // actual removal of last element
            val s: String = key_val(m)._2.right.get.last
            key_val.update(m, (key, Right(key_val(m)._2.right.get.init)))
            s
          }
        }
    }
  }

  /** Gets the range of elements from a list specified by the given key.
    *
    * @param key   the key whose list we are looking for
    * @param start the start index
    * @param stop  the end index
    * @return the list of strings between the start and end indices, or throw error if the key
    *         maps to a string value
    */
  def lrange(key: String, start: Int, stop: Int): List[String] = {
    val m = key_val.indexWhere(x => x._1 == key) // find the index key-value pair if it exists

    m match {
      case -1 => List[String]() // empty
      case _ =>
        if (key_val(m)._2.isLeft) { // string value
          throw new RuntimeException("WRONGTYPE: Operation against key holding wrong kind of value")
        } else if (key_val(m)._2.right.get.isEmpty) { // empty list value
          Nil
        } else { // non-empty list value
          key_val(m)._2.right.get.slice(start, stop + 1).toList // +1 because slice not inclusive
        }
    }
  }


  /** Return the length of the value associated with the given key.
    *
    * @param key the key we are looking at the value for
    * @return the length of the value mapped to the key, or throw error if a string value
    */
  def llen(key: String): Int = {
    val m = key_val.indexWhere(x => x._1 == key) // find the index key-value pair if it exists

    m match {
      case -1 => 0 // key does not exist case
      case _ =>
        if (key_val(m)._2.isLeft) { // error if key maps to string value
          throw new RuntimeException("WRONGTYPE: Operation against key holding wrong kind of value")
        } else { // key maps to list
          key_val(m)._2.right.get.length
        }
    }
  }

  /**
    * Clears the existing set of key values for this redis instance.
    */
  def flushall(): Unit = {
    key_val.clear()
  }

}

/**
  * Test object for `Redis` class, and its methods.
  */
object TestRedis extends App {

  val r = new Redis()

  /**
    * Initialize `redis` instance w/ values.
    */
  def init(): Unit = {
    r.flushall()
    r.set("a", "1")
    r.set("b", "2")
    r.lpush("lst", "third")
    r.lpush("lst", "second")
    r.lpush("lst", "first")
    r.rpush("lst", "last")
  }

  /**
    * Tests for the get method.
    */
  def testGet(): Unit = {
    init()
    println("a: " + r.get("a"))
    println("z: " + r.get("z"))
    try {
      r.get("lst")
    } catch {
      case e: RuntimeException => println("get 'lst': " + e.getMessage)
    }
  }

  /**
    * Tests for the lpop & length methods.
    */
  def testLpop(): Unit = {
    init()
    println("lst: length = " + r.llen("lst")) // start length
    println("lst: lpop-list = " +
      r.lpop("lst"), r.lpop("lst"), r.lpop("lst"), r.lpop("lst"))
    println("lst: length = " + r.llen("lst")) // end length
    println("donotexist: pop = " + r.lpop("donotexist"))
    try {
      r.lpop("a")
    } catch {
      case e: RuntimeException => println("lpop 'a': " + e.getMessage)
    }
  }

  /**
    * Tests for the rpop & length methods.
    */
  def testRpop(): Unit = {
    init()
    println("lst: length = " + r.llen("lst")) // start length
    println("lst: lpop-list = " +
      r.rpop("lst"), r.rpop("lst"), r.rpop("lst"), r.rpop("lst"))
    println("lst: length = " + r.llen("lst")) // end length
    println("donotexist: pop = " + r.lpop("donotexist"))
    try {
      r.rpop("a")
    } catch {
      case e: RuntimeException => println("rpop 'a': " + e.getMessage)
    }
  }

  /**
    * Tests fort the lrange method
    */
  def testLrange(): Unit = {
    init()
    println(r.lrange("lst", 0, 3))
    println(r.lrange("zebra", 0, 1))
    try {
      r.lrange("a", 1, 2)
    } catch {
      case e: RuntimeException => println("lrange 'a': " + e.getMessage)
    }
  }

  /**
    * Tests for flush all.
    */
  def testFlushAll(): Unit = {
    r.set("a", "one")
    println(r.get("a"))
    r.flushall()
    println(r.get("a"))
  }

  /** Prints a bunch of tests for manual testing/checking of Redis functionality. */
  //  testGet()
  //  testLpop()
  //  testRpop()
  //  testLrange()
  //  testFlushAll()
}