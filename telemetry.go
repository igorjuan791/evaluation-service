package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ctxKeySpan struct{}

// SpanContext representa o contexto W3C TraceContext (traceparent) do OpenTelemetry.
type SpanContext struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
}

// OTelAttribute representa um atributo chave-valor no formato OTLP JSON.
type OTelAttribute struct {
	Key   string        `json:"key"`
	Value OTelAttrValue `json:"value"`
}

// OTelAttrValue encapsula valores tipados do protocolo OTLP.
type OTelAttrValue struct {
	StringValue string `json:"stringValue,omitempty"`
	IntValue    string `json:"intValue,omitempty"`
	BoolValue   *bool  `json:"boolValue,omitempty"`
}

type metricKey struct {
	Method string
	Route  string
	Status int
}

// Telemetry gerencia Traces, Métricas, Logs OTLP e Chaos Engineering do evaluation-service.
type Telemetry struct {
	ServiceName  string
	OTLPEndpoint string
	HttpClient   *http.Client
	chaosEnabled atomic.Bool

	mu            sync.RWMutex
	requestCounts map[metricKey]uint64
	durationSumMs map[metricKey]float64
}

var globalTelemetry *Telemetry

// InitTelemetry inicializa o exportador OpenTelemetry (OTLP HTTP) e o coletor de métricas.
func InitTelemetry(defaultServiceName string) *Telemetry {
	svcName := os.Getenv("OTEL_SERVICE_NAME")
	if svcName == "" {
		svcName = defaultServiceName
	}
	endpoint := strings.TrimRight(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), "/")

	t := &Telemetry{
		ServiceName:  svcName,
		OTLPEndpoint: endpoint,
		HttpClient: &http.Client{
			Timeout: 2 * time.Second,
		},
		requestCounts: make(map[metricKey]uint64),
		durationSumMs: make(map[metricKey]float64),
	}
	globalTelemetry = t

	if endpoint != "" && os.Getenv("OTEL_SDK_DISABLED") != "true" {
		log.Printf("[OTel] OpenTelemetry inicializado para '%s' -> OTLP Endpoint: %s", svcName, endpoint)
		go t.periodicMetricsPushLoop()
	} else {
		log.Printf("[OTel] OpenTelemetry inicializado em modo local/no-op para '%s'", svcName)
	}

	return t
}

func randomHex(nBytes int) string {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("b", nBytes*2)
	}
	return hex.EncodeToString(b)
}

// ExtractOrNewSpan extrai o header W3C traceparent ou cria um novo TraceID/SpanID.
func ExtractOrNewSpan(r *http.Request) (context.Context, SpanContext) {
	tp := r.Header.Get("traceparent")
	var traceID, parentSpanID string
	if parts := strings.Split(tp, "-"); len(parts) == 4 && len(parts[1]) == 32 && len(parts[2]) == 16 {
		traceID = parts[1]
		parentSpanID = parts[2]
	} else {
		traceID = randomHex(16)
	}
	sc := SpanContext{
		TraceID:      traceID,
		SpanID:       randomHex(8),
		ParentSpanID: parentSpanID,
	}
	return context.WithValue(r.Context(), ctxKeySpan{}, sc), sc
}

// StartChildSpan inicia um span filho a partir do context.Context atual.
func StartChildSpan(ctx context.Context, spanName string, kind int, attrs map[string]string) (context.Context, func(err error)) {
	start := time.Now()
	parent, ok := ctx.Value(ctxKeySpan{}).(SpanContext)
	if !ok || parent.TraceID == "" {
		parent = SpanContext{
			TraceID: randomHex(16),
		}
	}
	child := SpanContext{
		TraceID:      parent.TraceID,
		SpanID:       randomHex(8),
		ParentSpanID: parent.SpanID,
	}
	childCtx := context.WithValue(ctx, ctxKeySpan{}, child)

	endFn := func(err error) {
		if globalTelemetry != nil {
			globalTelemetry.ExportSpanAsync(child, spanName, kind, start, time.Now(), attrs, err)
		}
	}
	return childCtx, endFn
}

// DetachSpanContext preserva o SpanContext em um novo context.Background() para goroutines assíncronas (ex.: SQS).
func DetachSpanContext(ctx context.Context) context.Context {
	sc, ok := ctx.Value(ctxKeySpan{}).(SpanContext)
	if !ok {
		return context.Background()
	}
	return context.WithValue(context.Background(), ctxKeySpan{}, sc)
}

// FormatTraceparent formata o SpanContext no padrão W3C TraceContext (00-traceId-spanId-01).
func FormatTraceparent(ctx context.Context) string {
	sc, ok := ctx.Value(ctxKeySpan{}).(SpanContext)
	if !ok || sc.TraceID == "" || sc.SpanID == "" {
		return fmt.Sprintf("00-%s-%s-01", randomHex(16), randomHex(8))
	}
	return fmt.Sprintf("00-%s-%s-01", sc.TraceID, sc.SpanID)
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.statusCode = code
	sr.ResponseWriter.WriteHeader(code)
}

// Middleware instrumenta todas as rotas HTTP com Distributed Tracing, Métricas e Chaos Injection.
func (t *Telemetry) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			t.MetricsHandler(w, r)
			return
		}
		if r.URL.Path == "/chaos/fail" {
			t.ChaosHandler(w, r)
			return
		}

		start := time.Now()
		ctx, sc := ExtractOrNewSpan(r)
		r = r.WithContext(ctx)

		w.Header().Set("traceparent", FormatTraceparent(ctx))
		rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}

		// Se o modo Chaos (Fault Injection) estiver ativo, falha rotas de negócio com HTTP 500
		if t.chaosEnabled.Load() && r.URL.Path != "/health" {
			rec.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(rec).Encode(map[string]string{
				"error":   "Falha simulada (Chaos Engineering / Self-Healing Test)",
				"service": t.ServiceName,
				"traceId": sc.TraceID,
			})
			durationMs := float64(time.Since(start).Microseconds()) / 1000.0
			t.RecordMetric(r.Method, r.URL.Path, http.StatusInternalServerError, durationMs)
			t.ExportSpanAsync(sc, fmt.Sprintf("%s %s", r.Method, r.URL.Path), 2, start, time.Now(), map[string]string{
				"http.method":      r.Method,
				"http.route":       r.URL.Path,
				"http.status_code": "500",
				"chaos.injected":   "true",
			}, fmt.Errorf("chaos fault injection HTTP 500"))
			t.ExportLogAsync(sc, "ERROR", fmt.Sprintf("[CHAOS] Retornando HTTP 500 proposital em %s %s (trace_id=%s)", r.Method, r.URL.Path, sc.TraceID))
			return
		}

		next.ServeHTTP(rec, r)

		durationMs := float64(time.Since(start).Microseconds()) / 1000.0
		t.RecordMetric(r.Method, r.URL.Path, rec.statusCode, durationMs)

		var spanErr error
		if rec.statusCode >= 500 {
			spanErr = fmt.Errorf("HTTP %d", rec.statusCode)
		}
		t.ExportSpanAsync(sc, fmt.Sprintf("%s %s", r.Method, r.URL.Path), 2, start, time.Now(), map[string]string{
			"http.method":      r.Method,
			"http.route":       r.URL.Path,
			"http.status_code": strconv.Itoa(rec.statusCode),
		}, spanErr)
	})
}

// ChaosHandler ativa ou desativa a injeção de falhas HTTP 500 para a demonstração de Self-Healing.
func (t *Telemetry) ChaosHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodPost || r.URL.Query().Get("enable") != "" {
		enableStr := r.URL.Query().Get("enable")
		enable := enableStr == "true" || enableStr == "1"
		t.chaosEnabled.Store(enable)
		log.Printf("[CHAOS] Modo de falha proposital em '%s' alterado para: %v", t.ServiceName, enable)
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"service":       t.ServiceName,
		"chaos_enabled": t.chaosEnabled.Load(),
	})
}

// RecordMetric contabiliza requisições e latência em memória de forma thread-safe.
func (t *Telemetry) RecordMetric(method, route string, status int, durationMs float64) {
	key := metricKey{Method: method, Route: route, Status: status}
	t.mu.Lock()
	t.requestCounts[key]++
	t.durationSumMs[key] += durationMs
	t.mu.Unlock()
}

// MetricsHandler expõe métricas no formato texto do Prometheus em /metrics.
func (t *Telemetry) MetricsHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	var sb strings.Builder
	sb.WriteString("# HELP http_server_requests_total Total de requisicoes HTTP processadas\n")
	sb.WriteString("# TYPE http_server_requests_total counter\n")

	t.mu.RLock()
	for k, count := range t.requestCounts {
		sb.WriteString(fmt.Sprintf(
			"http_server_requests_total{service=%q,method=%q,route=%q,status=%q} %d\n",
			t.ServiceName, k.Method, k.Route, strconv.Itoa(k.Status), count,
		))
	}
	sb.WriteString("# HELP http_server_duration_milliseconds_sum Soma da latencia das requisicoes HTTP em ms\n")
	sb.WriteString("# TYPE http_server_duration_milliseconds_sum counter\n")
	for k, sum := range t.durationSumMs {
		sb.WriteString(fmt.Sprintf(
			"http_server_duration_milliseconds_sum{service=%q,method=%q,route=%q,status=%q} %.3f\n",
			t.ServiceName, k.Method, k.Route, strconv.Itoa(k.Status), sum,
		))
	}
	t.mu.RUnlock()

	_, _ = io.WriteString(w, sb.String())
}

// ExportSpanAsync envia um Span no formato OTLP/HTTP JSON v1.0 para o OpenTelemetry Collector.
func (t *Telemetry) ExportSpanAsync(sc SpanContext, name string, kind int, start, end time.Time, attrs map[string]string, spanErr error) {
	if t.OTLPEndpoint == "" || os.Getenv("OTEL_SDK_DISABLED") == "true" {
		return
	}
	go func() {
		otelAttrs := make([]OTelAttribute, 0, len(attrs))
		for k, v := range attrs {
			otelAttrs = append(otelAttrs, OTelAttribute{
				Key:   k,
				Value: OTelAttrValue{StringValue: v},
			})
		}
		statusCode := 1
		statusMsg := ""
		if spanErr != nil {
			statusCode = 2
			statusMsg = spanErr.Error()
		}

		payload := map[string]interface{}{
			"resourceSpans": []map[string]interface{}{
				{
					"resource": map[string]interface{}{
						"attributes": []OTelAttribute{
							{Key: "service.name", Value: OTelAttrValue{StringValue: t.ServiceName}},
							{Key: "telemetry.sdk.name", Value: OTelAttrValue{StringValue: "opentelemetry"}},
							{Key: "telemetry.sdk.language", Value: OTelAttrValue{StringValue: "go"}},
						},
					},
					"scopeSpans": []map[string]interface{}{
						{
							"scope": map[string]interface{}{
								"name":    t.ServiceName + "-tracer",
								"version": "1.0.0",
							},
							"spans": []map[string]interface{}{
								{
									"traceId":           sc.TraceID,
									"spanId":            sc.SpanID,
									"parentSpanId":      sc.ParentSpanID,
									"name":              name,
									"kind":              kind,
									"startTimeUnixNano": strconv.FormatInt(start.UnixNano(), 10),
									"endTimeUnixNano":   strconv.FormatInt(end.UnixNano(), 10),
									"attributes":        otelAttrs,
									"status": map[string]interface{}{
										"code":    statusCode,
										"message": statusMsg,
									},
								},
							},
						},
					},
				},
			},
		}
		t.postOTLP("/v1/traces", payload)
	}()
}

// ExportLogAsync envia um Log estruturado via OTLP/HTTP JSON v1.0 correlacionado com TraceID e SpanID.
func (t *Telemetry) ExportLogAsync(sc SpanContext, severityText, body string) {
	if t.OTLPEndpoint == "" || os.Getenv("OTEL_SDK_DISABLED") == "true" {
		return
	}
	go func() {
		nowNano := strconv.FormatInt(time.Now().UnixNano(), 10)
		payload := map[string]interface{}{
			"resourceLogs": []map[string]interface{}{
				{
					"resource": map[string]interface{}{
						"attributes": []OTelAttribute{
							{Key: "service.name", Value: OTelAttrValue{StringValue: t.ServiceName}},
						},
					},
					"scopeLogs": []map[string]interface{}{
						{
							"scope": map[string]interface{}{"name": t.ServiceName + "-logger"},
							"logRecords": []map[string]interface{}{
								{
									"timeUnixNano": nowNano,
									"severityText": severityText,
									"body":         OTelAttrValue{StringValue: body},
									"traceId":      sc.TraceID,
									"spanId":       sc.SpanID,
									"attributes": []OTelAttribute{
										{Key: "service.name", Value: OTelAttrValue{StringValue: t.ServiceName}},
									},
								},
							},
						},
					},
				},
			},
		}
		t.postOTLP("/v1/logs", payload)
	}()
}

func (t *Telemetry) periodicMetricsPushLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		t.pushOTLPMetrics()
	}
}

func (t *Telemetry) pushOTLPMetrics() {
	t.mu.RLock()
	if len(t.requestCounts) == 0 {
		t.mu.RUnlock()
		return
	}
	nowNano := strconv.FormatInt(time.Now().UnixNano(), 10)
	dataPoints := make([]map[string]interface{}, 0, len(t.requestCounts))
	for k, count := range t.requestCounts {
		dataPoints = append(dataPoints, map[string]interface{}{
			"attributes": []OTelAttribute{
				{Key: "service", Value: OTelAttrValue{StringValue: t.ServiceName}},
				{Key: "method", Value: OTelAttrValue{StringValue: k.Method}},
				{Key: "route", Value: OTelAttrValue{StringValue: k.Route}},
				{Key: "status", Value: OTelAttrValue{StringValue: strconv.Itoa(k.Status)}},
			},
			"startTimeUnixNano": nowNano,
			"timeUnixNano":      nowNano,
			"asInt":             strconv.FormatUint(count, 10),
		})
	}
	t.mu.RUnlock()

	payload := map[string]interface{}{
		"resourceMetrics": []map[string]interface{}{
			{
				"resource": map[string]interface{}{
					"attributes": []OTelAttribute{
						{Key: "service.name", Value: OTelAttrValue{StringValue: t.ServiceName}},
					},
				},
				"scopeMetrics": []map[string]interface{}{
					{
						"scope": map[string]interface{}{"name": t.ServiceName + "-meter"},
						"metrics": []map[string]interface{}{
							{
								"name": "http_server_requests_total",
								"sum": map[string]interface{}{
									"aggregationTemporality": 2,
									"isMonotonic":            true,
									"dataPoints":             dataPoints,
								},
							},
						},
					},
				},
			},
		},
	}
	t.postOTLP("/v1/metrics", payload)
}

func (t *Telemetry) postOTLP(path string, payload interface{}) {
	b, err := json.Marshal(payload)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.OTLPEndpoint+path, bytes.NewReader(b))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.HttpClient.Do(req)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}
