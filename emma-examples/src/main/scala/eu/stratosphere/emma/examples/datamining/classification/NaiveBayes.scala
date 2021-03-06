package eu.stratosphere.emma.examples.datamining.classification

import breeze.linalg._
import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

/**
  * Trains a Naive Bayes Classifier.
  *
  * Currently, two model types - `bernoulli` and `multinomial` - are supported.
  */
class NaiveBayes(input: String, lambda: Double, modelType: String, rt: Engine)
  extends Algorithm(rt) {

  import eu.stratosphere.emma.examples.datamining.classification.NaiveBayes.Schema._
  import eu.stratosphere.emma.examples.datamining.classification.NaiveBayes._

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](NaiveBayes.Command.TRAINING),
    ns.get[Double](NaiveBayes.Command.LAMBDA),
    ns.get[String](NaiveBayes.Command.MODEL_TYPE),
    rt)

  val algorithm = emma.parallelize {
    val data = for (line <- read(input, new TextInputFormat[String]('\n'))) yield {
      val record = line.split(",").map(_.toDouble).toList
      LabeledVector(record.head, new DenseVector(record.slice(1, record.size).toArray))
    }

    // FIXME: replace with take(n)
    val dimension = data.find { _ => true }.get.vector.length

    val aggregated = for (group <- data.groupBy(_.label)) yield {
      val cnt = group.values.size
      val sum = group.values.fold(Vector.zeros[Double](dimension))(_.vector, _ + _)
      (group.key, cnt, sum)
    }

    val numDocuments = data.size
    val numLabels = aggregated.size
    val priorDenom = math.log(numDocuments + numLabels * lambda)

    val model = for ((label, count, vecSum) <- aggregated) yield {
      val priors = math.log(count + lambda) - priorDenom
      val evidenceDenom = modelType match {
        case MULTINOMIAL =>
          math.log(sum(vecSum) + lambda * dimension)
        case BERNOULLI =>
          math.log(count + 2.0 * lambda)
        case _ =>
          throw new UnknownError(s"Invalid modelType: $modelType.")
      }
      val evidences = vecSum.map(x => math.log(x + lambda) - evidenceDenom)
      (label, priors, evidences)
    }

    model
  }


  def run() = algorithm.run(rt)
}

object NaiveBayes {
  final val MULTINOMIAL: String = "multinomial"
  final val BERNOULLI: String = "bernoulli"

  object Command {
    // argument names
    val TRAINING = "training"
    val LAMBDA = "lambda"
    val MODEL_TYPE = "model_type"

  }

  class Command extends Algorithm.Command[NaiveBayes] {

    // algorithm names
    override def name = "naive-bayes"

    override def description = "Naive Bayes classification"

    override def setup(parser: Subparser) = {
      // basic setup
      super.setup(parser)

      parser.addArgument(Command.LAMBDA)
        .`type`[Double](classOf[Double])
        .dest(Command.LAMBDA)
        .metavar("EPSILON")
        .help("termination threshold")

      parser.addArgument(Command.TRAINING)
        .`type`[String](classOf[String])
        .dest(Command.TRAINING)
        .metavar("INPUT")
        .help("training data set")

      parser.addArgument(Command.MODEL_TYPE)
        .`type`[String](classOf[String])
        .dest(Command.MODEL_TYPE)
        .metavar("MODEL TYPE")
        .help("model type (bernoulli, multinomial)")

    }
  }

  object Schema {

    case class LabeledVector(label: Double, vector: Vector[Double])

  }

}
