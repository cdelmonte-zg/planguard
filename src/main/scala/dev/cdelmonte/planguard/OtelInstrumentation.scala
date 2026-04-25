package dev.cdelmonte.planguard

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.BatchSpanProcessor

import java.time.Duration

/**
 * Lazy-initialized OpenTelemetry SDK shared across detections.
 * Endpoint is read from OTEL_EXPORTER_OTLP_ENDPOINT (default localhost:4317).
 */
object OtelInstrumentation {

  private lazy val sdk: OpenTelemetry = {
    val endpoint = sys.env.getOrElse("OTEL_EXPORTER_OTLP_ENDPOINT",
                                     "http://localhost:4317")
    val serviceName = sys.env.getOrElse("OTEL_SERVICE_NAME", "spark-plan-guard")

    val resource = Resource.getDefault.merge(
      Resource.create(
        Attributes.of(AttributeKey.stringKey("service.name"), serviceName)))

    val exporter = OtlpGrpcSpanExporter.builder()
      .setEndpoint(endpoint)
      .setTimeout(Duration.ofSeconds(5))
      .build()

    val tracerProvider = SdkTracerProvider.builder()
      .setResource(resource)
      .addSpanProcessor(BatchSpanProcessor.builder(exporter)
        .setScheduleDelay(Duration.ofMillis(200))
        .build())
      .build()

    val sdk = OpenTelemetrySdk.builder()
      .setTracerProvider(tracerProvider)
      .build()

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      tracerProvider.shutdown().join(5, java.util.concurrent.TimeUnit.SECONDS)
      ()
    }))

    sdk
  }

  def tracer: Tracer = sdk.getTracer("dev.cdelmonte.planguard", "0.1.0")

  def forceFlush(): Unit = sdk match {
    case s: OpenTelemetrySdk =>
      s.getSdkTracerProvider.forceFlush().join(5, java.util.concurrent.TimeUnit.SECONDS)
      ()
    case _ =>
  }
}
