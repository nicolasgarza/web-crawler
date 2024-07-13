package storage

import (
	"context"
	pb "web-crawler/api/proto/storage"

	"google.golang.org/grpc"
)

type StorageClient struct {
	client pb.StorageServiceClient
}

func NewStorageClient(address string) (*StorageClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &StorageClient{
		client: pb.NewStorageServiceClient(conn),
	}, nil
}

func (sc *StorageClient) WriteResult(ctx context.Context, result []byte) error {
	_, err := sc.client.WriteResult(ctx, &pb.WriteResultRequest{Result: result})
	return err
}
