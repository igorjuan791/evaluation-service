.PHONY: help install-dev lint format test build check-all

help:
	@echo "Comandos disponíveis:"
	@echo "  make install-dev - Baixa dependências do Go"
	@echo "  make lint        - Executa análise estática / linter (go vet)"
	@echo "  make format      - Formata o código automaticamente (gofmt)"
	@echo "  make test        - Executa os testes unitários com cobertura"
	@echo "  make build       - Executa o build da imagem Docker"
	@echo "  make check-all   - Executa lint, test e build sequencialmente"

install-dev:
	go mod download
	go mod tidy

lint:
	go vet ./...

format:
	gofmt -s -w .

test:
	go test -v -race -coverprofile=coverage.out ./...

build:
	docker build -t evaluation-service:latest -f Dockerfile.evaluation .

check-all: lint test build
