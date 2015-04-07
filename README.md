# Emma

Emma is a declarative API for data-parallel programming. Emma algorithms can be executed natively on the Scala runtime or offloaded to a parallel dataflow engine like [Spark](https://spark.apache.org/) or [Flink](https://flink.apache.org).

To achieve optimal performance, the Emma compiler relies on a holistic view of the code as a mixture of control flow and dataflow expressions, as well as an algebraic foundation for data-parallel computation based on monads. 

For more detail about the design and implementation of the API and compiler pipeline, please refer to our SIGMOD paper [""](). 

<br>
## Programming Abstractions
Emma provides coarse-grained parallelism contracts through a dedicated type `DataBag[A]` representing a homogenous parallel collection over an element type `A`.
Computations over Emma bags can be written in declarative form similar to Select-From-Where SQL expressions using Scala `for`-comprehensions:

```scala
val zs = for (x <- xs; y <- ys; if p(x,y)) yield (x,y)
```

<br>
**Folds**

Emma exposes a fold operator as well as aliases for commonly used folds (e.g. count, exists, minBy).

```scala
val z = xs.fold(0, x => x, (x, y) => x + y)
val z = xs.sum() // alias for the above
```

<br>
**Grouping & Nesting**

```scala
val ys: DataBag[Grp[K,DataBag[A]]] = xs.groupBy(k)
```

As you can see in the above example, grouping in Emma produces nested DataBags. This is fundamentally different from Spark, Flink, and Hadoop MapReduce, where the group values have the type `Iterable[A]` or `Iterator[A]`. By introducing nested DataBags, we can avoid primitives like groupByKey, reduceByKey or aggregateByKey ans instead offer a declarative way of writing these operations.
To group a DataBag of tuples `(a, b)` and compute the sum over all `b` with the same `a`, we can simply write:

```scala
for (g <- xs.groupBy(_.a)) yield (g.key, g.values.sum())
```

Emma can recognize nested DataBag patterns at compile time and rewrite them into more efficient equivalent expressions using host-system primitives like aggregateByKey.

<br>
**Read and Write Operations**

Emma also provides operators to read and write data from a file system, as well as convertors for Scala `Seq` types:

```scala
// define schema
case class Point(id: Int, pos: Vector[Double])
case class Solution(cid: Int, p: Point)

// read data from a csv file
val points = read(inputUrl, CsvInputFormat[Point])

// initialize a databag from a for-comprehension
var ctrds = DataBag(for (i <- 1 to k) yield /*...*/)

// write data to filesystem as a csv file
write(outputUrl, CsvOutputFormat[Solution])(
  for (p <- points) yield {
    val c = ctrds.minBy(distanceTo(p)).get
    Solution(c.id, s.p)
})
```

## Examples/Quickstart




