package storage

import (
	"context"
	"encoding/json"
	"web-crawler/pkg/models"

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

func (cs *CassandraStorage) StorePage(ctx context.Context, url string, html string) error {
	query := "INSERT INTO pages (url, html) VALUES (?, ?)"
	return cs.session.Query(query, url, html).WithContext(ctx).Exec()
}

func (cs *CassandraStorage) GetPage(ctx context.Context, url string) (string, error) {
	var html string
	query := "SELECT html FROM pages WHERE url = ?"
	err := cs.session.Query(query, url).WithContext(ctx).Scan(&html)
	return html, err
}

func (cs *CassandraStorage) StoreCrawledPage(ctx context.Context, page *models.CrawledPage) error {
	// convert headers to json string
	headers, err := json.Marshal(page.Headers)
	if err != nil {
		return err
	}

	// convert extracted urls to json string
	extractedURLs, err := json.Marshal(page.ExtractedURLs)
	if err != nil {
		return err
	}

	query := `
        INSERT INTO crawled_pages (
            url, timestamp, status_code, headers, html, title, description, extracted_urls
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `

	return cs.session.Query(query,
		page.URL,
		page.Timestamp,
		page.StatusCode,
		string(headers),
		page.HTML,
		page.Title,
		page.Description,
		string(extractedURLs),
	).WithContext(ctx).Exec()
}
