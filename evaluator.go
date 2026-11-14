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
	"regexp"
	"strconv"
	"sync"
	"time"
)

const (
	// Tempo de vida do cache em segundos
	CACHE_TTL = 30 * time.Second
)

// validFlagName restringe o nome da flag a um allowlist de caracteres seguros
// antes de usá-lo para compor a URL de chamada aos outros serviços.
var validFlagName = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// getDecision executa a decisão propagando o contexto de trace OpenTelemetry
func (a *App) getDecision(reqCtx context.Context, userID, flagName string) (bool, error) {
	// 1. Obter os dados da flag (do cache ou dos serviços)
	info, err := a.getCombinedFlagInfo(reqCtx, flagName)
	if err != nil {
		return false, err
	}
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
		var info CombinedFlagInfo
		if err := json.Unmarshal([]byte(val), &info); err == nil {
			log.Printf("Cache HIT para flag '%s'", flagName)
			return &info, nil
		}
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
		if setErr != nil {
			log.Printf("Erro ao gravar cache para flag '%s': %v", flagName, setErr)
		}
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
	go func() {
		defer wg.Done()
		flagInfo, flagErr = a.fetchFlag(reqCtx, flagName)
	}()
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
	if !validFlagName.MatchString(flagName) {
		return nil, fmt.Errorf("nome de flag inválido: %q", flagName)
	}
	url := fmt.Sprintf("%s/flags/%s", a.FlagServiceURL, flagName)
	spanCtx, endSpan := StartChildSpan(reqCtx, "HTTP GET flag-service", 3, map[string]string{
		"http.method":  "GET",
		"http.url":     url,
		"peer.service": "flag-service",
	})
	var spanErr error
	defer func() { endSpan(spanErr) }()

	apiKey := os.Getenv("SERVICE_API_KEY")
	req, err := http.NewRequestWithContext(spanCtx, "GET", url, nil) // #nosec G704 -- host fixo (a.FlagServiceURL, config interna); flagName validado acima contra allowlist
	if err != nil {
		spanErr = err
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("traceparent", FormatTraceparent(spanCtx))

	resp, err := a.HttpClient.Do(req) // #nosec G704 -- ver justificativa acima; destino é sempre o flag-service interno
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
	if !validFlagName.MatchString(flagName) {
		return nil, fmt.Errorf("nome de flag inválido: %q", flagName)
	}
	url := fmt.Sprintf("%s/rules/%s", a.TargetingServiceURL, flagName)
	spanCtx, endSpan := StartChildSpan(reqCtx, "HTTP GET targeting-service", 3, map[string]string{
		"http.method":  "GET",
		"http.url":     url,
		"peer.service": "targeting-service",
	})
	var spanErr error
	defer func() { endSpan(spanErr) }()

	apiKey := os.Getenv("SERVICE_API_KEY") // Usa a mesma chave
	req, err := http.NewRequestWithContext(spanCtx, "GET", url, nil) // #nosec G704 -- host fixo (a.TargetingServiceURL, config interna); flagName validado acima contra allowlist
	if err != nil {
		spanErr = err
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("traceparent", FormatTraceparent(spanCtx))

	resp, err := a.HttpClient.Do(req) // #nosec G704 -- ver justificativa acima; destino é sempre o targeting-service interno
	if err != nil {
		spanErr = err
		return nil, fmt.Errorf("erro ao chamar targeting-service: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, &NotFoundError{flagName}
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
	rule := info.Rule.Rules
	if rule.Type == "PERCENTAGE" {
		percentage, ok := rule.Value.(float64)
		if !ok {
			log.Printf("Erro: valor da regra de porcentagem não é um número para a flag '%s'", info.Flag.Name)
			return false
		}
		userBucket := getDeterministicBucket(userID + info.Flag.Name)
		if float64(userBucket) < percentage {
			return true
		}
	}
	return false
}

func getDeterministicBucket(input string) int {
	hasher := sha1.New()
	hasher.Write([]byte(input))
	hash := hasher.Sum(nil)
	val := binary.BigEndian.Uint32(hash[:4])
	return int(val % 100)
}