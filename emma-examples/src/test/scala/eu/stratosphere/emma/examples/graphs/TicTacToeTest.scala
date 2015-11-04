package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.testutil.withRuntime

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TicTacToeTest extends FlatSpec with Matchers {

  "TicTacToe" should "compute the game-theoretical values" in withRuntime() { rt =>
    new TicTacToe().algorithm.run(rt)
  }
}
