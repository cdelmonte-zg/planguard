package dev.cdelmonte.planguard

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Black-box tests that exercise the full extension pipeline: a
 * `SparkSession` is created with `spark.sql.extensions=PlanGuardExtension`
 * and queries are run end-to-end. Each test verifies an observable effect
 * (throw / no throw / physical plan shape).
 */
class PlanGuardSpec
  extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("planguard-tests")
      .master("local[2]")
      .config("spark.sql.extensions", "dev.cdelmonte.planguard.PlanGuardExtension")
      .config("spark.driver.host", "localhost")
      // Disable OTel export (no collector in the test env).
      .config("spark.driverEnv.OTEL_SDK_DISABLED", "true")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  private def numbersA: DataFrame =
    spark.createDataFrame(Seq((1, 10), (2, 20), (3, 30))).toDF("a", "x")

  private def numbersB: DataFrame =
    spark.createDataFrame(Seq(Tuple1(15), Tuple1(25))).toDF("b")

  private def keyed(rows: Seq[(Int, String)]): DataFrame =
    spark.createDataFrame(rows).toDF("k", "v")

  // --- cartesian (Inner + None) -----------------------------------------

  test("blocks Inner join with no condition (forgotten ON clause)") {
    val ex = intercept[Exception] {
      numbersA.join(numbersB).collect()
    }
    val message = Option(ex.getMessage).getOrElse("") +
                  Option(ex.getCause).map(_.getMessage).getOrElse("")
    message should include ("cartesian product")
  }

  test("does NOT block explicit crossJoin (joinType = Cross is user intent)") {
    val rows = numbersA.crossJoin(numbersB).collect()
    rows.length shouldBe (3 * 2)
  }

  // --- non-equi join ----------------------------------------------------

  test("non-equi join executes (warns) and lowers to BroadcastNestedLoopJoin") {
    // Bind left/right once each so the column references in the condition
    // refer to the same DataFrame instances that are being joined.
    val left = numbersA
    val right = numbersB
    val joined = left.join(right, left("x") > right("b"))
    val plan = joined.queryExecution.executedPlan.toString
    val rows = joined.collect()
    plan should (include ("BroadcastNestedLoopJoin") or include ("CartesianProduct"))
    rows should not be empty
  }

  test("equi join is not flagged") {
    val left = keyed(Seq((1, "a"), (2, "b")))
    // Rename the join column on the right side to avoid the USING-column
    // form; we want a clean Inner+Some(EqualTo) shape.
    val right = keyed(Seq((1, "x"), (2, "y"))).withColumnRenamed("k", "k2")
    val joined = left.join(right, left("k") === right("k2"))
    joined.collect().length shouldBe 2
  }

  test("mixed equi + non-equi condition is not flagged (has at least one equi-key)") {
    val left = spark.createDataFrame(Seq((1, 10), (2, 20))).toDF("k", "x")
    val right = spark.createDataFrame(Seq((1, 5), (2, 15))).toDF("k", "y")
    val joined = left.join(right, left("k") === right("k") && left("x") > right("y"))
    joined.collect()
  }

  // --- collect_list / collect_set ---------------------------------------

  test("collect_list in aggregation: warns, completes") {
    val df = keyed(Seq((1, "a"), (1, "b"), (2, "c")))
    val agg = df.groupBy("k").agg(collect_list("v").as("vs"))
    agg.collect().length shouldBe 2
  }

  test("collect_set in aggregation: also warns (Collect[_] parent abstract class)") {
    val df = keyed(Seq((1, "a"), (1, "a"), (1, "b"), (2, "c")))
    val agg = df.groupBy("k").agg(collect_set("v").as("vs"))
    agg.collect().length shouldBe 2
  }

  test("aggregation without Collect[_] is not flagged") {
    val df = keyed(Seq((1, "a"), (1, "b"), (2, "c")))
    val agg = df.groupBy("k").agg(count("*").as("n"))
    agg.collect().length shouldBe 2
  }

  // --- plain queries (no patterns) --------------------------------------

  test("plain projection / filter does not throw") {
    spark.range(100).filter(col("id") > 50).count() shouldBe 49L
  }
}
