package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// Evento que será enviado para a fila
type EvaluationEvent struct {
	UserID    string    `json:"user_id"`
	FlagName  string    `json:"flag_name"`
	Result    bool      `json:"result"`
	Timestamp time.Time `json:"timestamp"`
}

// sendEvaluationEvent envia um evento para a fila SQS propagando o traceparent nos MessageAttributes
func (a *App) sendEvaluationEvent(reqCtx context.Context, userID, flagName string, result bool) {
	sqsCtx, endSpan := StartChildSpan(reqCtx, "sqs.send", 4, map[string]string{
		"messaging.system":           "aws_sqs",
		"messaging.destination.name": "toogle-events",
		"messaging.operation":        "publish",
		"peer.service":               "analytics-service",
	})
	var spanErr error
	defer func() { endSpan(spanErr) }()

	// Se a URL da fila não foi configurada, apenas loga localmente e sai.
	if a.SqsSvc == nil || a.SqsQueueURL == "" {
		log.Printf("[SQS_DISABLED] Evento: User '%s', Flag '%s', Result '%t'", userID, flagName, result)
		return
	}

	event := EvaluationEvent{
		UserID:    userID,
		FlagName:  flagName,
		Result:    result,
		Timestamp: time.Now().UTC(),
	}

	body, err := json.Marshal(event)
	if err != nil {
		spanErr = err
		log.Printf("Erro ao serializar evento SQS: %v", err)
		return
	}

	traceparent := FormatTraceparent(sqsCtx)

	// Envia a mensagem injetando o header W3C traceparent nos MessageAttributes do SQS
	_, err = a.SqsSvc.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(string(body)),
		QueueUrl:    aws.String(a.SqsQueueURL),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"traceparent": {
				DataType:    aws.String("String"),
				StringValue: aws.String(traceparent),
			},
		},
	})

	if err != nil {
		spanErr = err
		log.Printf("Erro ao enviar mensagem para SQS: %v", err)
	} else {
		log.Printf("Evento de avaliação enviado para SQS (Flag: %s, traceparent: %s)", flagName, traceparent)
	}
}