package main

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	// Tempo de vida do cache em segundos
	CACHE_TTL = 30 * time.Second
)

// getDecision executa a decisão propagando o contexto de trace OpenTelemetry
func (a *App) getDecision(reqCtx context.Context, userID, flagName string) (bool, error) {
	// 1. Obter os dados da flag (do cache ou dos serviços)
	info, err := a.getCombinedFlagInfo(reqCtx, flagName)
	if err != nil {
		return false, err
	}

	// 2. Executar a lógica de avaliação
	return a.runEvaluationLogic(info, userID), nil
}

// getCombinedFlagInfo busca os dados no Redis, com fallback para os microsserviços
func (a *App) getCombinedFlagInfo(reqCtx context.Context, flagName string) (*CombinedFlagInfo, error) {
	cacheKey := fmt.Sprintf("flag_info:%s", flagName)

	// 1. Tentar buscar do Cache (Redis) com Child Span OpenTelemetry
	redisGetCtx, endGetSpan := StartChildSpan(reqCtx, "redis.get", 3, map[string]string{
		"db.system":    "redis",
		"db.operation": "GET",
		"db.statement": "GET " + cacheKey,
	})
	val, err := a.RedisClient.Get(redisGetCtx, cacheKey).Result()
	endGetSpan(nil)
	if err == nil {
		// Cache HIT
		var info CombinedFlagInfo
		if err := json.Unmarshal([]byte(val), &info); err == nil {
			log.Printf("Cache HIT para flag '%s'", flagName)
			return &info, nil
		}
		// Se o unmarshal falhar, trata como cache miss
		log.Printf("Erro ao desserializar cache para flag '%s': %v", flagName, err)
	}

	log.Printf("Cache MISS para flag '%s'", flagName)
	// 2. Cache MISS - Buscar dos serviços propagando o TraceContext
	info, err := a.fetchFromServices(reqCtx, flagName)
	if err != nil {
		return nil, err
	}

	// 3. Salvar no Cache com Child Span OpenTelemetry
	jsonData, err := json.Marshal(info)
	if err == nil {
		redisSetCtx, endSetSpan := StartChildSpan(reqCtx, "redis.set", 3, map[string]string{
			"db.system":    "redis",
			"db.operation": "SET",
			"db.statement": "SET " + cacheKey,
		})
		setErr := a.RedisClient.Set(redisSetCtx, cacheKey, jsonData, CACHE_TTL).Err()
		endSetSpan(setErr)
	}

	return info, nil
}

// fetchFromServices busca dados do flag-service e targeting-service concorrentemente
func (a *App) fetchFromServices(reqCtx context.Context, flagName string) (*CombinedFlagInfo, error) {
	var wg sync.WaitGroup
	wg.Add(2)

	var flagInfo *Flag
	var ruleInfo *TargetingRule
	var flagErr, ruleErr error

	// Goroutine 1: Buscar do flag-service
	go func() {
		defer wg.Done()
		flagInfo, flagErr = a.fetchFlag(reqCtx, flagName)
	}()

	// Goroutine 2: Buscar do targeting-service
	go func() {
		defer wg.Done()
		ruleInfo, ruleErr = a.fetchRule(reqCtx, flagName)
	}()

	wg.Wait()

	if flagErr != nil {
		return nil, flagErr
	}
	if ruleErr != nil {
		log.Printf("Aviso: Nenhuma regra de segmentação encontrada para '%s'. Usando padrão.", flagName)
	}

	return &CombinedFlagInfo{
		Flag: flagInfo,
		Rule: ruleInfo,
	}, nil
}

// fetchFlag chama o flag-service propagando o header W3C traceparent
func (a *App) fetchFlag(reqCtx context.Context, flagName string) (*Flag, error) {
	url := fmt.Sprintf("%s/flags/%s", a.FlagServiceURL, flagName)
	spanCtx, endSpan := StartChildSpan(reqCtx, "HTTP GET flag-service", 3, map[string]string{
		"http.method":  "GET",
		"http.url":     url,
		"peer.service": "flag-service",
	})
	var spanErr error
	defer func() { endSpan(spanErr) }()

	apiKey := os.Getenv("SERVICE_API_KEY")
	req, err := http.NewRequestWithContext(spanCtx, "GET", url, nil)
	if err != nil {
		spanErr = err
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("traceparent", FormatTraceparent(spanCtx))

	resp, err := a.HttpClient.Do(req)
	if err != nil {
		spanErr = err
		return nil, fmt.Errorf("erro ao chamar flag-service: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, &NotFoundError{flagName}
	}
	if resp.StatusCode != http.StatusOK {
		spanErr = fmt.Errorf("status %d", resp.StatusCode)
		return nil, fmt.Errorf("flag-service retornou status %s", strconv.Itoa(resp.StatusCode))
	}

	body, _ := io.ReadAll(resp.Body)
	var flag Flag
	if err := json.Unmarshal(body, &flag); err != nil {
		spanErr = err
		return nil, fmt.Errorf("erro ao desserializar resposta do flag-service: %w", err)
	}
	return &flag, nil
}

// fetchRule chama o targeting-service propagando o header W3C traceparent
func (a *App) fetchRule(reqCtx context.Context, flagName string) (*TargetingRule, error) {
	url := fmt.Sprintf("%s/rules/%s", a.TargetingServiceURL, flagName)
	spanCtx, endSpan := StartChildSpan(reqCtx, "HTTP GET targeting-service", 3, map[string]string{
		"http.method":  "GET",
		"http.url":     url,
		"peer.service": "targeting-service",
	})
	var spanErr error
	defer func() { endSpan(spanErr) }()

	apiKey := os.Getenv("SERVICE_API_KEY")
	req, err := http.NewRequestWithContext(spanCtx, "GET", url, nil)
	if err != nil {
		spanErr = err
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("traceparent", FormatTraceparent(spanCtx))

	resp, err := a.HttpClient.Do(req)
	if err != nil {
		spanErr = err
		return nil, fmt.Errorf("erro ao chamar targeting-service: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, &NotFoundError{flagName} // Não é um erro fatal
	}
	if resp.StatusCode != http.StatusOK {
		spanErr = fmt.Errorf("status %d", resp.StatusCode)
		return nil, fmt.Errorf("targeting-service retornou status %s", strconv.Itoa(resp.StatusCode))
	}

	body, _ := io.ReadAll(resp.Body)
	var rule TargetingRule
	if err := json.Unmarshal(body, &rule); err != nil {
		spanErr = err
		return nil, fmt.Errorf("erro ao desserializar resposta do targeting-service: %w", err)
	}
	return &rule, nil
}

// runEvaluationLogic é onde a decisão é tomada
func (a *App) runEvaluationLogic(info *CombinedFlagInfo, userID string) bool {
	if info.Flag == nil || !info.Flag.IsEnabled {
		return false
	}

	if info.Rule == nil || !info.Rule.IsEnabled {
		return true
	}

	// 3. Processa a regra (só temos "PERCENTAGE" por enquanto)
	rule := info.Rule.Rules
	if rule.Type == "PERCENTAGE" {
		// Converte o 'value' (que é interface{}) para float64
		percentage, ok := rule.Value.(float64)
		if !ok {
			log.Printf("Erro: valor da regra de porcentagem não é um número para a flag '%s'", info.Flag.Name)
			return false
		}
		
		// Calcula o "bucket" do usuário (0-99)
		userBucket := getDeterministicBucket(userID + info.Flag.Name)
		
		if float64(userBucket) < percentage {
			return true
		}
	}

	return false
}

func getDeterministicBucket(input string) int {
	// Usamos SHA1 (rápido) e pegamos os primeiros 4 bytes
	hasher := sha1.New()
	hasher.Write([]byte(input))
	hash := hasher.Sum(nil)
	
	// Converte 4 bytes para um uint32
	val := binary.BigEndian.Uint32(hash[:4])
	
	// Retorna o módulo 100
	return int(val % 100)
}
