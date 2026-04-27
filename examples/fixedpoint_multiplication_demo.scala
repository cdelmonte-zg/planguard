// fixedpoint_multiplication_demo.scala
//
// Reproducible demonstration of FixedPoint rule multiplication in Catalyst.
//
// Why this is interesting: a Rule[LogicalPlan] registered inside a FixedPoint
// batch is invoked once per *batch iteration*, not once per matching node and
// not once per query. The batch keeps iterating as long as ANY rule in it
// rewrites the plan. So a side-effect counter inside our rule can fire many
// more times than the number of matching nodes in the tree -- the multiplier
// is "however many iterations the batch happened to need to converge".
//
// Run with:
//   spark-shell -i examples/fixedpoint_multiplication_demo.scala --master local[1]
//
// Verified on Spark 4.1.1 / Scala 2.13.17.

import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.optimizer.{
  BooleanSimplification, CombineFilters, ConstantFolding
}

// A side-effecting rule. It does NOT modify the plan; it only counts how many
// Filter nodes it sees on each invocation. This mirrors the article's scenario:
// a rule whose "real" purpose is to emit a metric (Prometheus counter, log
// line, audit event) on every detection, with the author wrongly assuming
// "one rule invocation per matching node".
val probeCounter = new AtomicInteger(0)

object CountingProbe extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.foreach {
      case _: Filter => probeCounter.incrementAndGet()
      case _ => // ignore
    }
    // Return the plan unchanged: this rule is observational, not transformational.
    plan
  }
}

// A custom RuleExecutor whose single batch is a FixedPoint(100) containing our
// probe AND three real Catalyst rules that *do* keep mutating the plan until
// convergence. Building our own executor (rather than relying on
// spark.experimental.extraOptimizations) gives us total control over the batch
// and makes the multiplication mechanically self-evident.
object DemoExecutor extends RuleExecutor[LogicalPlan] {
  // Using the protected Batch / FixedPoint via the inherited members of
  // RuleExecutor. FixedPoint(100) means "iterate up to 100 times or until the
  // plan stops changing".
  val batches: Seq[Batch] = Seq(
    Batch(
      "Demo",
      FixedPoint(100),
      // Order matters only for readability; the loop in RuleExecutor.execute
      // re-runs the whole list each iteration regardless.
      CountingProbe,
      ConstantFolding,
      BooleanSimplification,
      CombineFilters
    )
  )
}

// A query crafted so that the three "real" rules each have something to do:
// - adjacent Filters -> CombineFilters merges them (changes plan -> next iter)
// - constant arithmetic (1 + 1) -> ConstantFolding rewrites to a literal
// - tautological boolean (true AND ...) -> BooleanSimplification simplifies it
// The plan therefore needs SEVERAL iterations to reach a fixed point, and on
// every one of those iterations CountingProbe is invoked from scratch over
// whatever the current plan happens to be.
val df = spark.range(0, 100).toDF("id")
  .filter($"id" > (lit(1) + lit(1)))     // becomes id > 2 after ConstantFolding
  .filter(lit(true) && ($"id" < lit(50))) // becomes id < 50 after BooleanSimplification
  .filter($"id" =!= lit(7))               // adjacent filter, target for CombineFilters

// The analyzed plan is what we feed to the executor. We count Filter nodes in
// it BEFORE optimization; that is our N (the "true" number of detections the
// author of the rule expected to be reported).
val analyzed = df.queryExecution.analyzed
val nBefore = analyzed.collect { case _: Filter => 1 }.sum
println(s"\n[demo] Filters in analyzed plan (N = expected count): $nBefore")

// --- Run 1: rule lives inside a FixedPoint batch ------------------------------
probeCounter.set(0)
val optimized = DemoExecutor.execute(analyzed)
val fixedPointCount = probeCounter.get()
val nAfter = optimized.collect { case _: Filter => 1 }.sum

println(s"[demo] Filters in optimized plan: $nAfter")
println(s"[demo] CountingProbe invocations under FixedPoint: $fixedPointCount")
println(f"[demo] Multiplication ratio: ${fixedPointCount.toDouble / nBefore}%.2fx")

// --- Run 2: same rule applied ONCE, the way a check rule would be -------------
// SparkSessionExtensions.injectCheckRule wires a LogicalPlan => Unit function
// that runs once per query during analysis -- it is not part of any FixedPoint
// batch. We can't register one from inside `spark-shell -i` (extensions need
// to be on the classpath at JVM startup), but we can faithfully reproduce its
// semantics by invoking the rule a single time on the analyzed plan.
probeCounter.set(0)
CountingProbe(analyzed)
val checkRuleCount = probeCounter.get()
println(s"[demo] CountingProbe invocations as a check rule (one-shot): $checkRuleCount")

println(s"\n[demo] Summary: N=$nBefore, FixedPoint=$fixedPointCount, CheckRule=$checkRuleCount")
println("[demo] If FixedPoint > N, the multiplication phenomenon is confirmed.\n")

System.exit(0)
