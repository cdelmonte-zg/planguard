package dev.cdelmonte.planguard

import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.api.trace.{SpanKind, StatusCode}
import org.slf4j.LoggerFactory

import org.apache.spark.SparkException
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.{Expression, PythonUDF}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Collect}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan}

class PlanGuardExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // CheckRule (post-analysis, pre-optimization): runs once per query,
    // before any optimizer rewrite. This is the documented hook for
    // analysis-time validations and avoids the FixedPoint re-emission
    // problem of optimizer rules.
    extensions.injectCheckRule { session =>
      DetectDangerousPatterns(session)
    }
  }
}

/**
 * Inspection-only check: walks the analyzed LogicalPlan, emits an OTel span
 * and a log line for each dangerous pattern, and throws SparkException for
 * the cartesian case (the only structurally-blocking one).
 *
 * Signature is `LogicalPlan => Unit`, the type of an `injectCheckRule`
 * builder result. Because CheckRules run once per query, this is also the
 * right place for non-blocking inspection signals — there is no fan-out
 * across FixedPoint iterations.
 */
case class DetectDangerousPatterns(session: SparkSession)
  extends (LogicalPlan => Unit) {

  private val logger = LoggerFactory.getLogger(classOf[DetectDangerousPatterns])

  private val PATTERN = AttributeKey.stringKey("planguard.pattern")
  private val NODE = AttributeKey.stringKey("planguard.node")
  private val DETAIL = AttributeKey.stringKey("planguard.detail")

  override def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case Join(_, _, Inner, None, _) =>
        emit("cartesian_join", "Join",
             "Inner join without condition: cartesian product",
             error = true)
        throw new SparkException(
          "Join without condition detected: possible cartesian product"
        )

      case j @ Join(_, _, _, Some(_), _) if !isEquiJoin(j) =>
        emit("non_equi_join", "Join",
             "Non-equi join: will execute as nested-loop",
             error = false)
        logger.warn(
          "Non-equi join detected: will execute as nested-loop join"
        )

      case Filter(condition, _) if containsPythonUdf(condition) =>
        emit("python_udf_in_filter", "Filter",
             "Python UDF blocks predicate pushdown",
             error = false)
        logger.warn(
          "Python UDF in filter predicate: pushdown and optimization blocked"
        )

      case Aggregate(_, aggregations, _, _)
          if aggregations.exists(containsCollect) =>
        emit("collect_in_aggregate", "Aggregate",
             "collect_list/collect_set may create large in-memory buffers",
             error = false)
        logger.warn(
          "collect_list/collect_set in aggregation: may create large in-memory buffers"
        )

      case _ =>
    }
  }

  private def emit(pattern: String,
                   node: String,
                   detail: String,
                   error: Boolean): Unit = {
    val tracer = OtelInstrumentation.tracer
    val span = tracer.spanBuilder("catalyst.plan_guard.detect")
      .setSpanKind(SpanKind.INTERNAL)
      .setAllAttributes(Attributes.of(
        PATTERN, pattern,
        NODE, node,
        DETAIL, detail))
      .startSpan()
    try {
      if (error) {
        span.setStatus(StatusCode.ERROR, detail)
      }
    } finally {
      span.end()
    }
  }

  // ExtractEquiJoinKeys.unapply returns
  //   (joinType, leftKeys, rightKeys, otherCondition, conditionOnJoinKeys,
  //    leftChild, rightChild, joinHint)
  // Empty leftKeys means the planner has no equi-key bridging the two sides,
  // so the join falls back to nested-loop.
  private def isEquiJoin(j: Join): Boolean =
    ExtractEquiJoinKeys.unapply(j) match {
      case Some((_, leftKeys, _, _, _, _, _, _)) => leftKeys.nonEmpty
      case None => false
    }

  private def containsPythonUdf(expr: Expression): Boolean =
    expr.exists { case _: PythonUDF => true; case _ => false }

  // Collect[_] is the parent abstract class of CollectList, CollectSet, and
  // CollectTopK — all in org.apache.spark.sql.catalyst.expressions.aggregate,
  // all sharing the same large-buffer profile. In user-facing queries the
  // practically relevant cases are CollectList and CollectSet.
  private def containsCollect(expr: Expression): Boolean =
    expr.exists {
      case AggregateExpression(_: Collect[_], _, _, _, _) => true
      case _ => false
    }
}
