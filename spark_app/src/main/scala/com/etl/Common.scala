package com.etl

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Common {

  val HDFS = "hdfs://namenode:9000"

  def spark(appName: String): SparkSession =
    SparkSession.builder().appName(appName).getOrCreate()

  def banner(tag: String, lines: String*): Unit = {
    val bar = "=" * 70
    println(bar)
    println(s"[$tag]")
    lines.foreach(l => println(s"[$tag] $l"))
    println(bar)
  }

  def requireArg(args: Array[String], idx: Int, name: String): String =
    if (args.length > idx) args(idx)
    else throw new IllegalArgumentException(s"Missing required arg: $name")

  /**
   * Given a DataFrame with a `weight` column, add `_w_lower` and `_w_upper`
   * columns bracketing each row's slice of [0, total_weight).
   * The result is suitable for a range join against `rand() * total_weight`.
   */
  def withWeightBounds(df: DataFrame, weightCol: String): (DataFrame, Double) = {
    val w = Window.orderBy(monotonically_increasing_id())

    val bounds = df
      .withColumn("_w_lower",
        coalesce(sum(col(weightCol)).over(w.rowsBetween(Window.unboundedPreceding, -1)), lit(0.0)))
      .withColumn("_w_upper",
        sum(col(weightCol)).over(w.rowsBetween(Window.unboundedPreceding, 0)))

    val total = bounds.agg(sum(weightCol)).first().getDouble(0)
    (bounds, total)
  }

  /**
   * Weighted random sample: emit `n` rows whose `_rnd` column lands in
   * each reference row's weight interval.
   */
  def weightedJoin(
      factKeys: DataFrame,         // has column `_rnd`
      refWithBounds: DataFrame,    // has `_w_lower`, `_w_upper`
      refCols: Seq[String]
  ): DataFrame =
    factKeys.join(
      refWithBounds,
      factKeys("_rnd") >= refWithBounds("_w_lower") &&
      factKeys("_rnd") <  refWithBounds("_w_upper"),
      "left"
    ).select(factKeys.columns.filterNot(_ == "_rnd").map(col) ++ refCols.map(col): _*)
}