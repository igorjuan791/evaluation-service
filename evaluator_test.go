package main

import (
	"testing"
)

func TestGetDeterministicBucket(t *testing.T) {
	bucket1 := getDeterministicBucket("user123_flagA")
	bucket2 := getDeterministicBucket("user123_flagA")
	bucket3 := getDeterministicBucket("user456_flagA")

	// Deve ser determinístico
	if bucket1 != bucket2 {
		t.Errorf("Bucket deve ser determinístico para a mesma entrada: bucket1=%d, bucket2=%d", bucket1, bucket2)
	}

	// Deve estar no intervalo [0, 99]
	if bucket1 < 0 || bucket1 >= 100 {
		t.Errorf("Bucket fora do intervalo [0, 99]: %d", bucket1)
	}

	if bucket3 < 0 || bucket3 >= 100 {
		t.Errorf("Bucket fora do intervalo [0, 99]: %d", bucket3)
	}
}

func TestRunEvaluationLogic(t *testing.T) {
	app := &App{}

	// Caso 1: Flag desabilitada -> false
	infoDisabled := &CombinedFlagInfo{
		Flag: &Flag{Name: "test-flag", IsEnabled: false},
	}
	if app.runEvaluationLogic(infoDisabled, "user1") {
		t.Error("Flag desabilitada deveria retornar false")
	}

	// Caso 2: Flag habilitada sem regra -> true
	infoNoRule := &CombinedFlagInfo{
		Flag: &Flag{Name: "test-flag", IsEnabled: true},
		Rule: nil,
	}
	if !app.runEvaluationLogic(infoNoRule, "user1") {
		t.Error("Flag habilitada sem regra deveria retornar true")
	}

	// Caso 3: Regra de porcentagem 100% -> true
	info100Pct := &CombinedFlagInfo{
		Flag: &Flag{Name: "test-flag", IsEnabled: true},
		Rule: &TargetingRule{
			IsEnabled: true,
			Rules: Rule{
				Type:  "PERCENTAGE",
				Value: float64(100),
			},
		},
	}
	if !app.runEvaluationLogic(info100Pct, "user1") {
		t.Error("Regra 100% de rollout deveria sempre retornar true")
	}

	// Caso 4: Regra de porcentagem 0% -> false
	info0Pct := &CombinedFlagInfo{
		Flag: &Flag{Name: "test-flag", IsEnabled: true},
		Rule: &TargetingRule{
			IsEnabled: true,
			Rules: Rule{
				Type:  "PERCENTAGE",
				Value: float64(0),
			},
		},
	}
	if app.runEvaluationLogic(info0Pct, "user1") {
		t.Error("Regra 0% de rollout deveria retornar false")
	}
}
