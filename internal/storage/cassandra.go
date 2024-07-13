package storage

import (
	"context"
	pb "web-crawler/api/proto/storage"

	"github.com/gocql/gocql"
)

type CassandraStorage struct {
	session *gocql.Session
}

func NewCassandraStorage(hosts []string, keyspace string) (*CassandraStorage, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &CassandraStorage{session: session}, nil
}

func (cs *CassandraStorage) StorePage(ctx context.Context, req *pb.StorePageRequest) (*pb.StorePageResponse, error) {
	query := "INSERT INTO pages (url, html) VALUES (?, ?)"
	if err := cs.session.Query(query, req.Url, req.Html).Exec(); err != nil {
		return &pb.StorePageResponse{Success: false}, err
	}
	return &pb.StorePageResponse{Success: true}, nil
}

func (cs *CassandraStorage) GetPage(ctx context.Context, req *pb.GetPageRequest) (*pb.GetPageResponse, error) {
	var html string
	query := "SELECT html FROM pages WHERE url = ?"
	if err := cs.session.Query(query, req.Url).Scan(&html); err != nil {
		return nil, err
	}
	return &pb.GetPageResponse{Html: html}, nil
}
