package com.etl

import org.apache.spark.sql.{Column, SaveMode}
import org.apache.spark.sql.functions._

object GenerateMockData {

  // Hour -> weight. Index = hour of day. Peaks at 10 and 18.
  private val HourWeights: Array[Double] = Array(
    0.30, 0.15, 0.08, 0.06, 0.07, 0.15,
    0.40, 0.90, 1.60, 2.20, 2.40, 2.30,
    2.00, 1.80, 1.80, 1.90, 2.10, 2.50,
    2.70, 2.40, 1.90, 1.40, 0.90, 0.55
  )

  /** Map a uniform `_rnd` in [0,1) to an hour index. */
  private def hourFromUniform(colRnd: Column): Column = {
    val total = HourWeights.sum
    var cum = 0.0
    var expr: Column = lit(HourWeights.length - 1)
    for (i <- HourWeights.indices.reverse) {
      val upper = (cum + HourWeights(i)) / total
      expr = when(colRnd < lit(upper), lit(i)).otherwise(expr)
      cum += HourWeights(i)
    }
    expr
  }

  def main(args: Array[String]): Unit = {
    val pingsOut    = Common.requireArg(args, 0, "pings_output")
    val cashlessOut = Common.requireArg(args, 1, "cashless_output")
    val dateStr     = Common.requireArg(args, 2, "date (yyyy-MM-dd)")

    val nPings    = if (args.length > 3) args(3).toLong else 500000L
    val nCashless = if (args.length > 4) args(4).toLong else 100000L

    Common.banner("GenerateMockData",
      s"pings_out    = $pingsOut",
      s"cashless_out = $cashlessOut",
      s"date         = $dateStr",
      s"n_pings      = $nPings",
      s"n_cashless   = $nCashless")

    val spark = Common.spark("GenerateMockData")

    // ---- Reference tables ----
    val msisdns   = spark.read.parquet(s"${Common.HDFS}/data/ref/msisdns")
    val towers    = spark.read.parquet(s"${Common.HDFS}/data/ref/towers")
    val registers = spark.read.parquet(s"${Common.HDFS}/data/ref/registers")

    val nMsisdns   = msisdns.count()
    val nTowers    = towers.count()
    val nRegisters = registers.count()
    require(nMsisdns > 0 && nTowers > 0 && nRegisters > 0,
      "Reference tables empty — run the Jupyter notebook first")
    println(s"[ref] msisdns=$nMsisdns towers=$nTowers registers=$nRegisters")

    // Add a stable index to each reference table for the range joins
    val msisdnIdx   = msisdns.select(col("msisdn")).withColumn("msisdn_idx", monotonically_increasing_id())
    val towerIdx    = towers.select(col("cell_tower_id")).withColumn("tower_idx", monotonically_increasing_id())
    val registerIdx = registers.select(col("register_id")).withColumn("register_idx", monotonically_increasing_id())

    val dayStartUnix = spark
      .sql(s"SELECT unix_timestamp('$dateStr 00:00:00', 'yyyy-MM-dd HH:mm:ss') AS ts")
      .first().getLong(0)

    /** Compose a timestamp from an hour index + minute + second offsets. */
    def buildDatetime(h: Column, m: Column, s: Column): Column =
      to_timestamp(from_unixtime(lit(dayStartUnix) + h * 3600L + m * 60L + s))

    // =====================================================================
    // PINGS
    // =====================================================================
    val pingsFinal = spark.range(0, nPings)
      .withColumn("ping_id", concat(lit("P-"), lpad(col("id").cast("string"), 12, "0")))
      // Hour-weighted time
      .withColumn("hour_of_day", hourFromUniform(rand(seed = 41)))
      .withColumn("datetime",
        buildDatetime(
          col("hour_of_day"),
          (rand(seed = 42) * 60).cast("int"),
          (rand(seed = 43) * 60).cast("int")))
      // Uniform pick from the (already weighted) reference lists
      .withColumn("msisdn_idx",   (rand(seed = 44) * nMsisdns).cast("long"))
      .withColumn("tower_idx",    (rand(seed = 45) * nTowers).cast("long"))
      .join(msisdnIdx,   Seq("msisdn_idx"),   "left")
      .join(towerIdx,    Seq("tower_idx"),    "left")
      .select("ping_id", "datetime", "cell_tower_id", "msisdn")

    pingsFinal.write.mode(SaveMode.Overwrite).parquet(pingsOut)
    println(s"[GenerateMockData] wrote pings → $pingsOut")
    pingsFinal.show(5, truncate = false)

    // =====================================================================
    // CASHLESS
    // =====================================================================
    val cashlessFinal = spark.range(0, nCashless)
      .withColumn("log_id", concat(lit("L-"), lpad(col("id").cast("string"), 12, "0")))
      .withColumn("hour_of_day", hourFromUniform(rand(seed = 51)))
      .withColumn("datetime",
        buildDatetime(
          col("hour_of_day"),
          (rand(seed = 52) * 60).cast("int"),
          (rand(seed = 53) * 60).cast("int")))
      .withColumn("msisdn_idx",   (rand(seed = 54) * nMsisdns).cast("long"))
      .withColumn("register_idx", (rand(seed = 55) * nRegisters).cast("long"))
      .withColumn("amount",
        round(
          when(rand(seed = 56) < 0.95,
               lit(10)  + rand(seed = 57) * 490)
          .otherwise(
               lit(500) + rand(seed = 58) * 4500),
          2))
      .join(msisdnIdx,   Seq("msisdn_idx"),   "left")
      .join(registerIdx, Seq("register_idx"), "left")
      .select("log_id", "datetime", "register_id", "msisdn", "amount")

    cashlessFinal.write.mode(SaveMode.Overwrite).parquet(cashlessOut)
    println(s"[GenerateMockData] wrote cashless → $cashlessOut")
    cashlessFinal.show(5, truncate = false)

    // ---- Summary ----
    val pingsNulls = pingsFinal.filter(
      col("cell_tower_id").isNull || col("msisdn").isNull || col("datetime").isNull).count()
    val cashlessNulls = cashlessFinal.filter(
      col("register_id").isNull || col("msisdn").isNull || col("datetime").isNull).count()

    Common.banner("GenerateMockData",
      s"pings written    = ${pingsFinal.count()}",
      s"cashless written = ${cashlessFinal.count()}",
      s"pings nulls      = $pingsNulls",
      s"cashless nulls   = $cashlessNulls",
      "OK")

    spark.stop()
  }
}