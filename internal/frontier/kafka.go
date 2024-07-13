package frontier

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type URLFrontier struct {
	reader *kafka.Reader
	writer *kafka.Writer
}

func NewURLFrontier(brokers []string, topic string) *URLFrontier {
	return &URLFrontier{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			Topic:     topic,
			Partition: 0,
			MinBytes:  10e3, // 10KB
			MaxBytes:  10e6, // 10MB
		}),
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (uf *URLFrontier) AddURL(ctx context.Context, url string) error {
	return uf.writer.WriteMessages(ctx, kafka.Message{
		Value: []byte(url),
	})
}

func (uf *URLFrontier) GetURL(ctx context.Context) (string, error) {
	m, err := uf.reader.ReadMessage(ctx)
	if err != nil {
		return "", err
	}
	return string(m.Value), nil
}

func (uf *URLFrontier) Close() error {
	if err := uf.reader.Close(); err != nil {
		return err
	}
	return uf.writer.Close()
}

type ResultsWriter struct {
	writer *kafka.Writer
}

func NewResultsWriter(brokers []string, topic string) *ResultsWriter {
	return &ResultsWriter{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (rw *ResultsWriter) WriteResult(ctx context.Context, result []byte) error {
	return rw.writer.WriteMessages(ctx, kafka.Message{
		Value: result,
	})
}

func (rw *ResultsWriter) Close() error {
	return rw.writer.Close()
}
