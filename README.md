# Emma

Emma is a **declarative language** for data-parallel programming that is **deeply embedded into Scala** and 
supports **execution in multiple systems**, enabling programmers to reuse their algorithms across different systems without having to rewrite them.

Programs in Emma are compiled to programs in the host system after an **extensive cycle of optimizations** to guarantee executable code that is comparable (or better!) to handwritten programs.

Currently, supported systems include [Flink 0.9](https://flink.apache.org) and [Spark 1.2](https://spark.apache.org/) in addition to native Scala execution.

<br>
## Programming Abstractions & API
Emma's main abstraction is similar to the one used by Spark and Flink: a type that represents homogeneous collections with bag semantics - called DataBag.
The operators we present here are not abstract. The semantics of each operator are given directly as method definitions in Scala. This feature facilitates rapid prototyping, as it allows the prorammer to incrementally develop, test, and debug the code at small scale locally as a pure Scala program.

<br>
**Declarative Select-Project-Join Abstractions**

Emma's API misses some basic binary operators like join and cross. This is a language design choice; in-
stead, we provide the operations map, flatMap and withFilter which enables the programmer to use Scala's for-comprehension syntax
to write Select-Project-Join expressions in a declarative way:

```scala
val zs = for (x <- xs; y <- ys; if p(x,y)) yield (x,y)
```

<br>
**Folds**

Native computation on DataBag values is allowed only by means of structural recursion. To that end, Emma 
exposes a fold operator as well as aliases for commonly used folds (e.g. count, exists, minBy). 
Summing up a bag of numbers, for example, can be written as:

```scala
val z = xs.fold(0, x => x, (x, y) => x + y)
val z = xs.sum() // alias for the above
```

<br>
**Grouping & Nesting**

Emma's grouping operator introduces nesting:

```scala
val ys: DataBag[Grp[K,DataBag[A]]] = xs.groupBy(k)
```

The resulting databag contains groups of input elements that share the same key. The `Grp` type has two components – a
group key: K, and group values: DataBag[A]. This is fundamentally different from Spark, Flink, and Hadoop MapReduce, where the group values have the type Iterable[A] or Iterator[A]. An ubiquitous support for DataBag nesting allows us to hide primitives like groupByKey, reduceByKey and aggregateByKey, which might appear to be the same, behind a uniform *“groupBy and fold”* programming model.
To group a bag of (a, b) tuples by a and compute the sum of b for each group, for example, we can write:

```scala
for (g <- xs.groupBy(_.a)) yield (g.key, g.values.sum())
```

Due to the deep embedding approach we commit to, we can recognize nested DataBag patterns like the one above at compile time and rewrite them into more efficient equivalent expressions using host-system primitives like aggregateByKey.

<br>
**Read and Write Operations**

To interface with the core DataBag abstraction, the language also provides operators to read and write data from a file system, as well as convertors for Scala `Seq` types:

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

## Examples




