// Demo: two logically equivalent multi-way joins, written in different
// orders, produce different PHYSICAL plans on Spark 4.1.1 — because
// rule-based rewriting does not reorder joins unless the cost-based
// optimizer is enabled and statistics are available.
//
// Run with:
//   $SPARK_HOME/bin/spark-shell -i join_order_demo.scala
//
// (Requires Spark 4.1.1 binary distribution. AQE + CBO are explicitly
// disabled below so the comparison is between two "pure" rule-based plans.)

import org.apache.spark.sql.functions._

// Disable adaptive execution and the cost-based optimizer so we observe
// only what the rule-based pipeline produces.
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.cbo.enabled", "false")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "false")

// Lower the broadcast threshold so only the very small B is broadcastable.
// This forces C to use sort-merge — making the operator-type difference
// between the two orderings visually clear.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10240")  // 10 KB

// Three datasets with deliberately different row counts. spark.range
// gives Catalyst a precise rowCount stat so the broadcast/sort-merge
// decision is deterministic.
val A = spark.range(1, 1000000).toDF("a").withColumn("k", $"a" % 100)   // ~1M rows
val B = spark.range(1, 100).toDF("b").withColumn("k", $"b")             // tiny, broadcastable
val C = spark.range(1, 50000).toDF("c").withColumn("k", $"c" % 100)     // ~50k rows, NOT broadcastable

// Two logically equivalent queries — same join keys, same final result —
// but written in different orders.
val plan1 = A.join(B, "k").join(C, "k")   // (A ⋈ B) ⋈ C
val plan2 = A.join(C, "k").join(B, "k")   // (A ⋈ C) ⋈ B

println("=" * 78)
println("Form 1:  A.join(B, \"k\").join(C, \"k\")     — (A ⋈ B) ⋈ C")
println("=" * 78)
plan1.explain(false)

println("\n" + "=" * 78)
println("Form 2:  A.join(C, \"k\").join(B, \"k\")     — (A ⋈ C) ⋈ B")
println("=" * 78)
plan2.explain(false)

// Sanity check: both produce the same row count (logical equivalence).
println("\n" + "=" * 78)
println("Logical equivalence check (row counts must match):")
println("=" * 78)
println(s"  plan1.count = ${plan1.count()}")
println(s"  plan2.count = ${plan2.count()}")

System.exit(0)
