package org.apache.spark.planguard

import dev.cdelmonte.planguard.DetectDangerousPatterns

import org.apache.spark.SparkException
import org.apache.spark.api.python.{PythonEvalType, PythonFunction}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.types.BooleanType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Direct invocation of the rule on a hand-built LogicalPlan that contains a
 * PythonUDF. We construct the UDF with `func = null.asInstanceOf[PythonFunction]`
 * — same trick Spark's own AnalysisSuite uses; valid only because the rule
 * inspects expression class without evaluating it.
 *
 * Lives under `org.apache.spark.*` because `PythonFunction` is `private[spark]`.
 */
class PythonUdfRuleSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("planguard-pythonudf-test")
      .master("local[2]")
      .config("spark.driver.host", "localhost")
      .config("spark.driverEnv.OTEL_SDK_DISABLED", "true")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("rule warns on PythonUDF in Filter and does not throw") {
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

    rule(filterPlan)  // must not throw
  }

  test("rule throws on Inner+None Join (cartesian)") {
    import org.apache.spark.sql.catalyst.plans.Inner
    import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint}

    val left = spark.range(2).queryExecution.analyzed
    val right = spark.range(2).queryExecution.analyzed
    val joinPlan = Join(left, right, Inner, None, JoinHint.NONE)

    val rule = DetectDangerousPatterns(spark)
    val ex = intercept[SparkException] {
      rule(joinPlan)
    }
    assert(ex.getMessage.contains("cartesian product"))
  }
}
