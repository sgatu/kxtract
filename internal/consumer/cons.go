package consumer

import (
	"github.com/sgatu/kxtract/internal/config"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer struct {
	client *kgo.Client
}

func NewConsumer(cfg *config.ExtractionRequest) {
	opts := make([]kgo.Opt, 0)
	client, err := kgo.NewClient(opts...)
}
