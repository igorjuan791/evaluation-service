package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthHandler(t *testing.T) {
	app := &App{}

	req, err := http.NewRequest("GET", "/health", nil)
	if err != nil {
		t.Fatalf("Erro ao criar requisição: %v", err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(app.healthHandler)
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Status esperado %d, recebido %d", http.StatusOK, rr.Code)
	}

	var response map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
		t.Fatalf("Erro ao decodificar JSON: %v", err)
	}

	if response["status"] != "ok" {
		t.Errorf("Status retornado deveria ser 'ok', recebido '%s'", response["status"])
	}
}

func TestTelemetryAndChaosHandler(t *testing.T) {
	app := &App{}
	telemetry := InitTelemetry("evaluation-service-test")

	mux := http.NewServeMux()
	mux.HandleFunc("/health", app.healthHandler)
	mux.HandleFunc("/evaluate", app.evaluationHandler)
	handler := telemetry.Middleware(mux)

	// 1. Ativar Chaos Fault Injection
	reqChaosOn, _ := http.NewRequest("POST", "/chaos/fail?enable=true", nil)
	rrChaosOn := httptest.NewRecorder()
	handler.ServeHTTP(rrChaosOn, reqChaosOn)
	if rrChaosOn.Code != http.StatusOK {
		t.Fatalf("Esperado 200 em /chaos/fail, recebido %d", rrChaosOn.Code)
	}

	// 2. /evaluate deve retornar HTTP 500 durante o Chaos
	reqEval, _ := http.NewRequest("GET", "/evaluate?user_id=u1&flag_name=f1", nil)
	rrEval := httptest.NewRecorder()
	handler.ServeHTTP(rrEval, reqEval)
	if rrEval.Code != http.StatusInternalServerError {
		t.Errorf("Esperado HTTP 500 em /evaluate com chaos ativo, recebido %d", rrEval.Code)
	}

	// 3. /health continua retornando 200 (evita que livenessProbe reinicie antes do Self-Healing)
	reqHealth, _ := http.NewRequest("GET", "/health", nil)
	rrHealth := httptest.NewRecorder()
	handler.ServeHTTP(rrHealth, reqHealth)
	if rrHealth.Code != http.StatusOK {
		t.Errorf("Esperado HTTP 200 em /health mesmo com chaos ativo, recebido %d", rrHealth.Code)
	}
}

