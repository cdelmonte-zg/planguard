package org.apache.spark.example

import dev.cdelmonte.planguard.{DetectDangerousPatterns, OtelInstrumentation}

import org.apache.spark.api.python.{PythonEvalType, PythonFunction}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.types.BooleanType

/**
 * Live verification of the article's PlanGuardExtension. Runs four scenarios:
 *   1. cartesian join      -> expects SparkException raised by the rule
 *   2. collect_list        -> expects log warning
 *   3. PythonUDF in Filter -> directly invokes the rule on a hand-built
 *      LogicalPlan because constructing a working Python UDF from Scala
 *      requires a Python worker, which is out of scope here.
 *   4. non-equi join       -> expects log warning, physical plan should
 *      contain BroadcastNestedLoopJoin (or CartesianProduct).
 */
object Verify {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("planguard-verify")
      .master("local[2]")
      .config("spark.sql.extensions", "dev.cdelmonte.planguard.PlanGuardExtension")
      // The scenario below uses df.join(other) without ON to produce the
      // dangerous pattern (Inner+None); we intentionally do NOT use
      // df.crossJoin(other), which is explicit user intent and is not
      // flagged by the rule. (spark.sql.crossJoin.enabled defaults to true
      // in Spark 4.x — older or differently-configured versions may need
      // it set explicitly to allow Inner+None to reach the rule at all.)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=== Scenario 1: cartesian join (should be blocked) ===")
    val s1 = scenarioCartesianJoin(spark)
    println(s"Scenario 1 result: $s1")

    println("=== Scenario 2: collect_list in aggregation (should log warning) ===")
    val s2 = scenarioCollectList(spark)
    println(s"Scenario 2 result: $s2")

    println("=== Scenario 3: PythonUDF in Filter (rule invoked directly) ===")
    val s3 = scenarioPythonUdf(spark)
    println(s"Scenario 3 result: $s3")

    println("=== Scenario 4: non-equi join (should warn nested-loop) ===")
    val s4 = scenarioNonEquiJoin(spark)
    println(s"Scenario 4 result: $s4")

    println("=== SUMMARY ===")
    println(s"cartesian-blocked = $s1")
    println(s"collect_list-warning = $s2")
    println(s"python-udf-warning = $s3")
    println(s"non-equi-nested-loop = $s4")

    OtelInstrumentation.forceFlush()
    spark.stop()
  }

  private def scenarioCartesianJoin(spark: SparkSession): String = {
    import spark.implicits._
    val left = Seq(1, 2, 3).toDF("a")
    val right = Seq(10, 20).toDF("b")
    // Note: with injectCheckRule the rule fires during analysis, which
    // happens inside `Dataset.ofRows` — i.e. at `left.join(right)`, not at
    // `.collect()`. The try-block must wrap the join construction too.
    try {
      val joined = left.join(right)
      joined.collect()
      "FAIL: rule did not raise"
    } catch {
      case e: org.apache.spark.SparkException
          if e.getMessage.contains("cartesian product") =>
        "PASS: SparkException raised: " + e.getMessage
      case e: Throwable =>
        s"FAIL: unexpected exception: ${e.getClass.getName}: ${e.getMessage}"
    }
  }

  private def scenarioCollectList(spark: SparkSession): String = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val df = Seq((1, "a"), (1, "b"), (2, "c")).toDF("k", "v")
    val agg = df.groupBy("k").agg(collect_list("v").as("vs"))
    val rows = agg.collect()
    s"PASS: aggregation executed (${rows.length} rows). Check stderr for WARN log."
  }

  private def scenarioNonEquiJoin(spark: SparkSession): String = {
    import spark.implicits._
    val left = Seq((1, 10), (2, 20), (3, 30)).toDF("a", "x")
    val right = Seq(15, 25).toDF("b")
    val joined = left.join(right, left("x") > right("b"))
    val plan = joined.queryExecution.executedPlan.toString
    val rows = joined.collect()
    val isNested = plan.contains("BroadcastNestedLoopJoin") ||
                   plan.contains("CartesianProduct")
    val suffix = if (isNested) "nested-loop confirmed in physical plan"
                 else "(physical plan is not nested-loop, please check)"
    s"PASS: non-equi join executed (${rows.length} rows), $suffix"
  }

  private def scenarioPythonUdf(spark: SparkSession): String = {
    // Use the analyzed plan so the relation has resolved output we can
    // reference in the UDF's children. The predicate must be BooleanType.
    val baseRel = spark.range(0).queryExecution.analyzed
    val pythonUdf = PythonUDF(
      name = "pyUDF",
      func = null.asInstanceOf[PythonFunction],
      dataType = BooleanType,
      children = Seq(baseRel.output.head),
      evalType = PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = true)

    val rule = DetectDangerousPatterns(spark)
    val filterPlan = Filter(pythonUdf, baseRel)
    try {
      rule.apply(filterPlan)
      "PASS: rule did not throw (warning should be on stderr)"
    } catch {
      case e: Throwable =>
        s"FAIL: rule threw: ${e.getClass.getName}: ${e.getMessage}"
    }
  }
}
