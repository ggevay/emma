package eu.stratosphere.emma.examples.graphs

import eu.stratosphere.emma.api._
//import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.api.Algorithm
import eu.stratosphere.emma.runtime.Engine


class TicTacToe() extends Algorithm[Unit]() {

  import eu.stratosphere.emma.examples.graphs.TicTacToe._

  def run(rt: Engine) = algorithm run rt

  val algorithm = emma.parallelize
  {
    val allEdgeTargets = DataBag(Seq(GameState(0,0), GameState(0,0), GameState(2,2)))
    println("allEdgeTargets: ", allEdgeTargets.fetch())

    val inDegrees = for {
      g <- allEdgeTargets.groupBy {x => x}
    } yield (g.key, g.values.count())

    println("inDegrees: ", inDegrees.fetch())
  }
}

object TicTacToe {
  case class GameState(whites: Int, blacks: Int) // Bitsets for white and black stones (9 bits in each, as the board is 3x3)
}
