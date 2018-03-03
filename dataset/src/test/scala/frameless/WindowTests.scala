package frameless

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{ functions => sfuncs }
import frameless.functions.WindowFunctions._
import org.scalacheck.Prop
import org.scalacheck.Prop._

class WindowTests extends TypedDatasetSuite {
  test("window creation") {
    def prop[
    A : TypedEncoder,
    B : TypedEncoder : CatalystOrdered
    ](data: X2[A, B]): Prop = {
      val ds = TypedDataset.create(Seq(data))

      val untypedWindow = Window.orderBy("b").partitionBy("a")
      val typedWindow = TypedWindow.orderBy(ds[B]('b)).partitionBy(ds('a))

      println(untypedWindow)
      println(typedWindow.untyped)
      passed
    }

    check(prop[Int, String] _)
  }



  test("dense rank") {
    def prop[
      A : TypedEncoder,
      B : TypedEncoder : CatalystOrdered
    ](data: Vector[X2[A, B]]): Prop = {
      val ds = TypedDataset.create(data)

      val untypedWindow = Window.partitionBy("a").orderBy("b")
      val typedWindow = TypedWindow.orderBy(ds[B]('b))
          .partitionBy(ds('a))

      val untyped = TypedDataset.createUnsafe[X3[A, B, Int]](ds.toDF()
        .withColumn("c", sfuncs.dense_rank().over(untypedWindow))
      ).collect().run().toVector

      val denseRankWindow = denseRank(typedWindow)

      val typed = ds.withColumn[X3[A, B, Int]](denseRankWindow)
        .collect().run().toVector

      typed ?= untyped
    }

    check(forAll(prop[Int, String] _))
    check(forAll(prop[SQLDate, SQLDate] _))
    check(forAll(prop[String, Boolean] _))
  }
}
