package main

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
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

// getDecision é o wrapper principal
func (a *App) getDecision(userID, flagName string) (bool, error) {
	info, err := a.getCombinedFlagInfo(flagName)
	if err != nil {
		return false, err
	}
	return a.runEvaluationLogic(info, userID), nil
}

// getCombinedFlagInfo busca os dados no Redis, com fallback para os microsserviços
func (a *App) getCombinedFlagInfo(flagName string) (*CombinedFlagInfo, error) {
	cacheKey := fmt.Sprintf("flag_info:%s", flagName)
	val, err := a.RedisClient.Get(ctx, cacheKey).Result()
	if err == nil {
		var info CombinedFlagInfo
		if err := json.Unmarshal([]byte(val), &info); err == nil {
			log.Printf("Cache HIT para flag '%s'", flagName)
			return &info, nil
		}
		log.Printf("Erro ao desserializar cache para flag '%s': %v", flagName, err)
	}
	log.Printf("Cache MISS para flag '%s'", flagName)
	info, err := a.fetchFromServices(flagName)
	if err != nil {
		return nil, err
	}
	jsonData, err := json.Marshal(info)
	if err == nil {
		if err := a.RedisClient.Set(ctx, cacheKey, jsonData, CACHE_TTL).Err(); err != nil {
			log.Printf("Erro ao gravar cache para flag '%s': %v", flagName, err)
		}
	}
	return info, nil
}

// fetchFromServices busca dados do flag-service e targeting-service concorrentemente
func (a *App) fetchFromServices(flagName string) (*CombinedFlagInfo, error) {
	var wg sync.WaitGroup
	wg.Add(2)
	var flagInfo *Flag
	var ruleInfo *TargetingRule
	var flagErr, ruleErr error
	go func() {
		defer wg.Done()
		flagInfo, flagErr = a.fetchFlag(flagName)
	}()
	go func() {
		defer wg.Done()
		ruleInfo, ruleErr = a.fetchRule(flagName)
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

// fetchFlag (função helper)
func (a *App) fetchFlag(flagName string) (*Flag, error) {
	if !validFlagName.MatchString(flagName) {
		return nil, fmt.Errorf("nome de flag inválido: %q", flagName)
	}
	url := fmt.Sprintf("%s/flags/%s", a.FlagServiceURL, flagName)
	apiKey := os.Getenv("SERVICE_API_KEY")
	req, _ := http.NewRequest("GET", url, nil) // #nosec G704 -- host fixo (a.FlagServiceURL, config interna); flagName validado acima contra allowlist
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := a.HttpClient.Do(req) // #nosec G704 -- ver justificativa acima; destino é sempre o flag-service interno
	if err != nil {
		return nil, fmt.Errorf("erro ao chamar flag-service: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, &NotFoundError{flagName}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("flag-service retornou status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var flag Flag
	if err := json.Unmarshal(body, &flag); err != nil {
		return nil, fmt.Errorf("erro ao desserializar resposta do flag-service: %w", err)
	}
	return &flag, nil
}

func (a *App) fetchRule(flagName string) (*TargetingRule, error) {
	if !validFlagName.MatchString(flagName) {
		return nil, fmt.Errorf("nome de flag inválido: %q", flagName)
	}
	url := fmt.Sprintf("%s/rules/%s", a.TargetingServiceURL, flagName)
	apiKey := os.Getenv("SERVICE_API_KEY") // Usa a mesma chave
	req, _ := http.NewRequest("GET", url, nil) // #nosec G704 -- host fixo (a.TargetingServiceURL, config interna); flagName validado acima contra allowlist
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := a.HttpClient.Do(req) // #nosec G704 -- ver justificativa acima; destino é sempre o targeting-service interno
	if err != nil {
		return nil, fmt.Errorf("erro ao chamar targeting-service: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, &NotFoundError{flagName}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("targeting-service retornou status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var rule TargetingRule
	if err := json.Unmarshal(body, &rule); err != nil {
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
