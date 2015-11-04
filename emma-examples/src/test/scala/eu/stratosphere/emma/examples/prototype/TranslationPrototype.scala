package eu.stratosphere.emma.examples.prototype

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime


class TranslationPrototype(rt: runtime.Engine) extends Algorithm(rt) {

  def run() = {
    import TranslationPrototype._

    emma.parallelize {
      val allEdgeTargets = DataBag(Seq(GameState(0,0), GameState(0,0), GameState(2,2)))
      println("allEdgeTargets: ", allEdgeTargets.fetch()) // seems about OK

      val inDegrees = for {
        g <- allEdgeTargets.groupBy { x => x }
      } yield (g.key, g.values.count())

      println("inDegrees: ", inDegrees.fetch()) // not OK: empty

    }.run(rt)

  }
}

object TranslationPrototype {
  case class GameState(whites: Int, blacks: Int) // Bitsets for white and black stones (9 bits in each, as the board is 3x3)
}









