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
