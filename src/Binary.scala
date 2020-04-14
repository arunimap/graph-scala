import breeze.plot._

object Binary extends App {

  /**
    * Recursively converts the given unsigned integer to an n-bit binary string
    * 'bits' specifies the min length of the binary string, adding leading zeroes when needed
    *
    * @param x    the unsigned integer value to be converted
    * @param bits the minimum number of bits in the resulting binary string
    * @return the given integer as a binary string
    */
  def toBinary(x: Int, bits: Int): String = {
    x match {
      case 0 | 1 => "0" * (bits - 1) + s"$x"
      case _ => toBinary(x / 2, bits - 1) + s"${x % 2}"
    }
  }

  /**
    * Return the weight of the given string by counting the number of 1s in the string
    *
    * @param b The string we are finding the weight of
    * @return the weight of the string, or -1 if given an invalid binary string
    */
  def weight(b: String): Int = {
    b.count(x => x != '1' & x != '0') match {
      case 0 => b.count(x => x == '1')
      case _ => -1
    }
  }

  // Produce a plot of the weight of binary numbers from 0 to 1024.

  /* Test cases */
  println("120: " + "binary = " + toBinary(120, 8))
  println("120: " + "weight = " + weight(toBinary(120, 8)))
  println("abc10: " + "weight = " + weight("abc10"))

  // What is the 32-bit binary for 1,234,567,890?
  println("1,234,567,890: " + "binary = " + toBinary(1234567890, 32))
  // Weight of 32-bit binary for 1,234,567,890
  println("1,234,567,890: " + "weight = " + weight(toBinary(1234567890, 32)))

  println((0 to 1024).toList)
  println((0 to 1024).map(x => weight(toBinary(x, 32))))
}
