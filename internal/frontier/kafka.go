package frontier

import (
	"context"
	pb "web-crawler/api/proto/frontier"

	"google.golang.org/grpc"

	"github.com/segmentio/kafka-go"
)

type FrontierClient struct {
	client pb.FrontierServiceClient
}

func NewFrontierClient(address string) (*FrontierClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &FrontierClient{
		client: pb.NewFrontierServiceClient(conn),
	}, nil
}

func (fc *FrontierClient) AddURL(ctx context.Context, url string) error {
	_, err := fc.client.AddURL(ctx, &pb.AddURLRequest{Url: url})
	return err
}

func (fc *FrontierClient) GetURL(ctx context.Context) (string, error) {
	resp, err := fc.client.GetURL(ctx, &pb.GetURLRequest{})
	if err != nil {
		return "", err
	}
	return resp.Url, nil
}

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
