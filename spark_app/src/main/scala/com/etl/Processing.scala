package com.etl

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object Processing {

  def main(args: Array[String]): Unit = {
    val pingsIn    = Common.requireArg(args, 0, "pings_input")
    val cashlessIn = Common.requireArg(args, 1, "cashless_input")
    val outputDir  = Common.requireArg(args, 2, "output_dir")

    Common.banner("Processing",
      s"pings_in    = $pingsIn",
      s"cashless_in = $cashlessIn",
      s"output_dir  = $outputDir")

    val spark = Common.spark("Processing")

    val pings    = spark.read.parquet(pingsIn)
    val cashless = spark.read.parquet(cashlessIn)
    val regDic   = spark.read.parquet(s"${Common.HDFS}/data/ref/registers")
      .select("register_id", "cell_tower_id")

    val cashlessByTower = cashless.join(regDic, Seq("register_id"), "left")

    // Add both `hour` and `date` — works for single-day and multi-day runs
    def withTimeBuckets(df: DataFrame): DataFrame =
      df.withColumn("hour", date_trunc("hour", col("datetime")))
        .withColumn("date", to_date(col("datetime")))

    // ---- 1) Pings per hour per tower ----
    val pingsByHourTower = withTimeBuckets(pings)
      .groupBy("date", "hour", "cell_tower_id")
      .agg(
        count("*").as("ping_count"),
        countDistinct("msisdn").as("distinct_msisdns")
      )
      .orderBy("date", "hour", "cell_tower_id")

    pingsByHourTower.write.mode(SaveMode.Overwrite)
      .parquet(s"$outputDir/pings_by_hour_tower")

    // ---- 2) Cashless per hour per register ----
    val cashlessByHourRegister = withTimeBuckets(cashless)
      .groupBy("date", "hour", "register_id")
      .agg(
        count("*").as("log_count"),
        round(sum("amount"), 2).as("total_amount"),
        round(avg("amount"), 2).as("avg_amount"),
        round(min("amount"), 2).as("min_amount"),
        round(max("amount"), 2).as("max_amount"),
        round(expr("percentile_approx(amount, 0.5)"), 2).as("p50_amount"),
        round(expr("percentile_approx(amount, 0.95)"), 2).as("p95_amount"),
        countDistinct("msisdn").as("distinct_msisdns")
      )
      .orderBy("date", "hour", "register_id")

    cashlessByHourRegister.write.mode(SaveMode.Overwrite)
      .parquet(s"$outputDir/cashless_by_hour_register")

    // ---- 3) Cashless per hour per tower (via dictionary) ----
    val cashlessByHourTower = withTimeBuckets(cashlessByTower)
      .groupBy("date", "hour", "cell_tower_id")
      .agg(
        count("*").as("log_count"),
        round(sum("amount"), 2).as("total_amount"),
        round(avg("amount"), 2).as("avg_amount"),
        round(min("amount"), 2).as("min_amount"),
        round(max("amount"), 2).as("max_amount"),
        round(expr("percentile_approx(amount, 0.5)"), 2).as("p50_amount"),
        round(expr("percentile_approx(amount, 0.95)"), 2).as("p95_amount"),
        countDistinct("register_id").as("distinct_registers"),
        countDistinct("msisdn").as("distinct_msisdns")
      )
      .orderBy("date", "hour", "cell_tower_id")

    cashlessByHourTower.write.mode(SaveMode.Overwrite)
      .parquet(s"$outputDir/cashless_by_hour_tower")

    // ---- 4) Cashless per hour per msisdn ----
    val cashlessByHourMsisdn = withTimeBuckets(cashless)
      .groupBy("date", "hour", "msisdn")
      .agg(
        count("*").as("log_count"),
        round(sum("amount"), 2).as("total_amount"),
        round(avg("amount"), 2).as("avg_amount"),
        round(max("amount"), 2).as("max_amount")
      )
      .orderBy("date", "hour", "msisdn")

    cashlessByHourMsisdn.write.mode(SaveMode.Overwrite)
      .parquet(s"$outputDir/cashless_by_hour_msisdn")

    // ---- Summary ----
    val summary = Seq(
      ("pings_by_hour_tower",       pingsByHourTower),
      ("cashless_by_hour_register", cashlessByHourRegister),
      ("cashless_by_hour_tower",    cashlessByHourTower),
      ("cashless_by_hour_msisdn",   cashlessByHourMsisdn)
    )

    summary.foreach { case (name, df) =>
      println(s"[Processing] $name: ${df.count()} rows")
      df.show(5, truncate = false)
    }

    Common.banner("Processing",
      summary.map { case (n, df) => s"$n = ${df.count()}" } :+ "OK": _*)

    spark.stop()
  }
}