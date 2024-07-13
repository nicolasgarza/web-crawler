package models

import (
	"net/http"
	"time"
)

type CrawledPage struct {
	URL           string
	Timestamp     time.Time
	StatusCode    int
	Headers       *http.Header
	HTML          string
	Title         string
	Description   string
	ExtractedURLs []string
}
