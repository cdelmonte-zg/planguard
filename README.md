# PlanGuard

End-to-end verification of the example code described in the article
*"The hidden DSL inside Catalyst"*: a check-rule registered through
`SparkSessionExtensions.injectCheckRule` that detects four dangerous
patterns in the analyzed logical plan (cartesian join, non-equi join →
nested-loop, Python UDF in filter, `collect_list`/`collect_set` in
aggregation) and emits OpenTelemetry spans with metadata about each
detection.

The bundled observability stack (`otel/`) turns those spans into:
- **traces** viewable in **Tempo** (and **Jaeger** as a backup);
- **Prometheus counters / histograms** via the collector's `spanmetrics`
  connector, browsable in **Grafana**.

## Prerequisites

| Tool | Tested version |
|---|---|
| JDK | 17 |
| Maven | 3.x |
| Docker | 29.x |
| docker-compose | 1.29 (v1) or `docker compose` (v2 plugin) |
| Spark | 4.1.1 binary, path exported as `$SPARK_HOME` |

Scala is not required as a system binary: `scala-maven-plugin` pulls
Scala 2.13.17 from Maven Central at build time (matched to Spark 4.1.1's
own Scala version). The Spark 4.1.1 binary
distribution is downloaded once from
`https://dlcdn.apache.org/spark/spark-4.1.1/spark-4.1.1-bin-hadoop3.tgz`.

## Layout

```
planguard/
├── pom.xml                                       # Spark 4.1.1, OTel SDK, shade
├── src/main/scala/
│   ├── dev/cdelmonte/planguard/
│   │   ├── PlanGuardExtension.scala              # SparkSessionExtensions + CheckRule
│   │   └── OtelInstrumentation.scala             # lazy SDK init (env-driven)
│   └── org/apache/spark/example/                 # under org.apache.spark.* to access
│       └── Verify.scala                          #   the private[spark] PythonFunction type
├── src/test/scala/
│   ├── dev/cdelmonte/planguard/PlanGuardSpec.scala         # black-box, end-to-end
│   └── org/apache/spark/planguard/PythonUdfRuleSpec.scala  # white-box, hand-built plan
├── python/
│   └── demo_planguard.py                         # PySpark demo: same 4 scenarios
└── otel/
    ├── docker-compose.yml                        # collector+jaeger+tempo+prom+grafana
    ├── otel-collector-config.yaml                # pipelines + spanmetrics connector
    ├── tempo.yaml
    ├── prometheus.yml
    └── grafana/provisioning/datasources/datasources.yml
```

## Quick start

```bash
# 1. Bring up the OTel stack (in the background)
cd otel && docker-compose up -d && cd ..

# 2. Build + run the test suite (11 tests, ~5s)
mvn -q test

# ...or build only (produces the shaded fat-jar with OTel + kotlin-stdlib relocated)
mvn -q -DskipTests=true package

# Point SPARK_HOME at any Spark 4.1.1 binary distribution
export SPARK_HOME=../spark-4.1.1-bin-hadoop3

# 3a. Run via Scala
"$SPARK_HOME/bin/spark-submit" \
  --class org.apache.spark.example.Verify \
  --master 'local[2]' \
  --conf spark.sql.extensions=dev.cdelmonte.planguard.PlanGuardExtension \
  --conf spark.driverEnv.OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
  --conf spark.driverEnv.OTEL_SERVICE_NAME=spark-plan-guard \
  target/planguard-0.1.0-shaded.jar

# 3b. ...or via PySpark (--jars loads the extension at JVM start)
"$SPARK_HOME/bin/spark-submit" \
  --jars target/planguard-0.1.0-shaded.jar \
  python/demo_planguard.py
```

> **Bootstrap note for PySpark.** Setting `spark.jars` inside
> `SparkSession.builder.config(...)` is **too late**: the JVM is already
> running and Spark has already looked up `spark.sql.extensions` on the
> classpath, producing a `ClassNotFoundException`. The JAR must be on the
> classpath at JVM-launch time. Two ways to do that:
> 1. pass `--jars target/...shaded.jar` to `spark-submit` (as in 3b);
> 2. or, if running via plain `python demo.py` (with `pyspark` in a venv),
>    set `PYSPARK_SUBMIT_ARGS="--jars /abs/path/to.jar pyspark-shell"`
>    **before** `import pyspark`. The included `demo_planguard.py` does this
>    automatically at the top of the file.
>
> Same constraint applies to Delta Lake, Iceberg, and any other extension —
> it is a JVM bootstrap detail, not specific to this project.

## Verification scenarios

| # | Scenario | Expected result |
|---|---|---|
| 1 | `df.join(other).collect()` (Inner join, no `ON` clause) | `SparkException: Join without condition detected: possible cartesian product` |
| 2 | `left.join(right, col("x") > col("b"))` | `WARN DetectDangerousPatterns: Non-equi join...`; physical plan = `BroadcastNestedLoopJoin` |
| 3 | `df.groupBy(...).agg(collect_list(...))` | `WARN DetectDangerousPatterns: collect_list ...` + execution completes |
| 4 | `df.filter(my_udf(col(...)))` with a Python `@udf` | `WARN DetectDangerousPatterns: Python UDF ...` + execution completes |

The app prints a final `=== SUMMARY ===` block with `PASS` / `FAIL` /
`UNEXPECTED` for each scenario.

## What to inspect in the stack

```bash
# Prometheus counters produced by the spanmetrics connector
curl -sG http://localhost:9090/api/v1/query \
  --data-urlencode 'query=sum by (planguard_pattern) (catalyst_calls_total)'

# Traces indexed in Tempo
docker exec planguard-grafana wget -qO- \
  'http://tempo:3200/api/search?tags=service.name%3Dspark-plan-guard&limit=10'

# Raw collector log (debug exporter)
docker logs planguard-otel-collector | grep planguard.pattern
```

| Service | URL | Notes |
|---|---|---|
| Grafana | http://localhost:3000 | anonymous Admin, datasources auto-provisioned |
| Prometheus | http://localhost:9090 | scrapes `otel-collector:8889` every 5s |
| Jaeger | http://localhost:16686 | dev-only quick-look |
| OTel `/metrics` | http://localhost:8889/metrics | output of the spanmetrics connector |
| OTLP gRPC | `localhost:4317` | endpoint for the Spark JVM |

## Cleanup

```bash
cd otel && docker-compose down -v
```

`-v` also removes the `tempo-data` and `prom-data` volumes.

## Notes

**Why `Verify.scala` lives under `org.apache.spark.example`.** Scenario 3
hand-builds a `PythonUDF(name, null: PythonFunction, …)` so the rule can
be invoked on a synthetic plan without a real Python worker. The
`PythonFunction` trait is `private[spark]`, so only code under the
`org.apache.spark.*` package may reference its type. The package
placement is a deliberate workaround for that visibility constraint —
not a claim that the file is part of Spark. The same trick is used by
Spark's own test suite (e.g. `AnalysisSuite.scala`).
