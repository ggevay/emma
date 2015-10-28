package eu.stratosphere.emma.examples.prototype

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.codegen.testschema.IMDBEntry
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime
import eu.stratosphere.emma.testutil._

class TranslationPrototype(rt: runtime.Engine) extends Algorithm(rt) {

  def run() = {
    val algo = emma.parallelize
    {
      val imdbTop100 = read(materializeResource("/cinema/imdb.csv"), new CSVInputFormat[IMDBEntry])

      val jj = for {
        imdb <- imdbTop100
        that <- imdbTop100
        if {
          val IMDBEntry(_, rating, _, _, year) = imdb
          (year, rating)
        } == (that.year, that.rating)
      } yield that

      val out = jj.groupBy(x=>x)

      //write("/tmp/xxx", new CSVOutputFormat[IMDBEntry])(out)
    }

    algo run rt
  }
}

object TranslationPrototype {
}