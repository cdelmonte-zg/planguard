"""
demo_planguard.py — uses PlanGuardExtension from PySpark, attaching it
on-the-fly to a SparkSession without modifying the installed Spark.

Run it in either of the two ways:

  A) spark-submit (the JVM is started by Spark; pass the JAR with --jars):

       /path/to/spark-4.1.1-bin-hadoop3/bin/spark-submit \\
           --jars target/planguard-0.1.0-shaded.jar \\
           python/demo_planguard.py

  B) plain Python (requires `pip install pyspark==4.1.1` in a venv):
     this script sets PYSPARK_SUBMIT_ARGS *before* importing pyspark,
     so the JAR is on the classpath when the JVM is launched.

       python python/demo_planguard.py

The OTel stack (`cd otel && docker-compose up -d`) is optional: if it is
not running, the script still completes — the SDK simply retries the
export silently.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


HERE = Path(__file__).resolve().parent
DEFAULT_JAR = HERE.parent / "target" / "planguard-0.1.0-shaded.jar"
JAR_PATH = Path(os.environ.get("PLANGUARD_JAR", DEFAULT_JAR)).resolve()
OTLP_ENDPOINT = os.environ.get(
    "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"
)


def _ensure_jar_on_classpath() -> None:
    """
    If we are not already running under spark-submit, prepend --jars to
    PYSPARK_SUBMIT_ARGS. Under spark-submit the JVM has already been
    launched and this change is a no-op; in that case the user must have
    passed --jars themselves.
    """
    if not JAR_PATH.exists():
        sys.exit(
            f"JAR not found: {JAR_PATH}\n"
            f"Build it first with:  mvn -q -DskipTests=true package"
        )

    existing = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    # If --jars is already present (e.g. we were invoked from pyspark/jupyter),
    # leave the value alone.
    if "--jars" not in existing:
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--jars {JAR_PATH} {existing or 'pyspark-shell'}"
        )


_ensure_jar_on_classpath()

# Import pyspark *after* setting PYSPARK_SUBMIT_ARGS.
from pyspark.sql import SparkSession                 # noqa: E402
from pyspark.sql.functions import col, collect_list, udf  # noqa: E402
from pyspark.sql.types import BooleanType            # noqa: E402
from py4j.protocol import Py4JJavaError              # noqa: E402


def build_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("demo-planguard")
        .master("local[2]")
        # Register the extension: a single line, no system-level install.
        .config("spark.sql.extensions",
                "dev.cdelmonte.planguard.PlanGuardExtension")
        # Note: spark.sql.crossJoin.enabled defaults to true on Spark 4.x.
        # On older or differently-configured Sparks, set it to "true" here
        # so an Inner+None join reaches our rule before Spark's own check.
        # The OTel SDK inside the rule reads endpoint and service name
        # from the driver process environment.
        .config("spark.driverEnv.OTEL_EXPORTER_OTLP_ENDPOINT", OTLP_ENDPOINT)
        .config("spark.driverEnv.OTEL_SERVICE_NAME", "spark-plan-guard-py")
        .getOrCreate()
    )


def scenario_cartesian(spark: SparkSession) -> str:
    left = spark.createDataFrame([(1,), (2,), (3,)], ["a"])
    right = spark.createDataFrame([(10,), (20,)], ["b"])
    try:
        # Inner join with no condition (forgotten ON). df.crossJoin(other)
        # would be explicit user intent and is not flagged by the rule.
        left.join(right).collect()
        return "FAIL: the rule did not block the cartesian"
    except Py4JJavaError as e:
        msg = str(e.java_exception).splitlines()[0]
        if "cartesian product" in msg:
            return f"PASS: blocked by the rule — {msg}"
        return f"UNEXPECTED: {msg}"


def scenario_collect_list(spark: SparkSession) -> str:
    df = spark.createDataFrame(
        [(1, "a"), (1, "b"), (2, "c")], ["k", "v"]
    )
    rows = (
        df.groupBy("k")
        .agg(collect_list("v").alias("vs"))
        .collect()
    )
    return f"PASS: aggregation completed ({len(rows)} rows). Check stderr for WARN."


def scenario_non_equi_join(spark: SparkSession) -> str:
    """Join with a non-equi condition (>, <, between, ...) → nested-loop."""
    left = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["a", "x"])
    right = spark.createDataFrame([(15,), (25,)], ["b"])
    # Condition: x > b — non-equi, cannot be lowered to a hash join.
    joined = left.join(right, left.x > right.b)
    plan = joined._jdf.queryExecution().executedPlan().toString()
    rows = joined.collect()
    is_nested = "BroadcastNestedLoopJoin" in plan or "CartesianProduct" in plan
    suffix = "nested-loop confirmed in physical plan" if is_nested else \
             "(physical plan is not nested-loop, please check)"
    return f"PASS: non-equi join executed ({len(rows)} rows), {suffix}. Check stderr for WARN."


def scenario_python_udf(spark: SparkSession) -> str:
    @udf(returnType=BooleanType())
    def is_even(x):
        return x is not None and x % 2 == 0

    rows = (
        spark.range(10)
        .toDF("n")
        .filter(is_even(col("n")))
        .collect()
    )
    return f"PASS: filter with @udf executed ({len(rows)} rows). Check stderr for WARN."


def main() -> None:
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=== Scenario 1: cartesian join (cross join, no condition) ===")
    s1 = scenario_cartesian(spark)
    print(s1)

    print("=== Scenario 2: collect_list in aggregation ===")
    s2 = scenario_collect_list(spark)
    print(s2)

    print("=== Scenario 3: Python UDF in filter ===")
    s3 = scenario_python_udf(spark)
    print(s3)

    print("=== Scenario 4: non-equi join → nested-loop ===")
    s4 = scenario_non_equi_join(spark)
    print(s4)

    print("=== SUMMARY ===")
    print(f"cartesian-blocked    = {s1}")
    print(f"collect_list-warning = {s2}")
    print(f"python-udf-warning   = {s3}")
    print(f"non-equi-nested-loop = {s4}")

    spark.stop()


if __name__ == "__main__":
    main()
