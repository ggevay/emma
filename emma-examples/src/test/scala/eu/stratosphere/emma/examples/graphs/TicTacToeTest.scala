package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.testutil._

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TicTacToeTest extends FlatSpec with Matchers {
  import TicTacToe._

  "TicTacToe" should "compute the game-theoretical values" in withRuntime() { rt =>
    val outPath = tempPath("TicTacToe")
    new TicTacToe(outPath, rt).run()
    val result = read(outPath, new CSVInputFormat[VertexWithValue]).fetch().map { _.vc }
    // Could be compared to the native rt, but it is too slow, so we use a "checksum" instead
    result.collect { case u: Undefined => u } should be ('empty)
    result.collect { case Win(depth) => depth }.sum should equal (8697)
    result.collect { case Loss(depth) => depth }.sum should equal (4688)
    result.collect { case Count(cnt) => cnt }.sum should equal (3495)
  }
}
