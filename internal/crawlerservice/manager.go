package crawlerservice

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"
	pb "web-crawler/api/proto/crawler"
	"web-crawler/internal/frontier"
	"web-crawler/internal/storage"

	"google.golang.org/protobuf/types/known/timestamppb"
)

type CrawlerService struct {
	pb.UnimplementedCrawlerServiceServer
	coordinator *Coordinator
}

func NewCrawlerService(numScrapers int, frontierAddress string, storageAddress string) (*CrawlerService, error) {
	coordinator, err := NewCoordinator(numScrapers, frontierAddress, storageAddress)
	if err != nil {
		return nil, err
	}
	return &CrawlerService{
		coordinator: coordinator,
	}, nil
}

func (s *CrawlerService) Scrape(ctx context.Context, req *pb.ScrapeRequest) (*pb.ScrapeResponse, error) {
	scraper := s.coordinator.getAvailableScraper()
	defer s.coordinator.releaseScraper(scraper)

	result, err := scraper.Scrape(req.Url)
	if err != nil {
		return nil, err
	}

	return convertToScrapeResponse(result), nil
}

func (s *CrawlerService) Run() {
	s.coordinator.Run()
}

type Coordinator struct {
	urlFrontier   *frontier.FrontierClient
	resultsWriter *storage.StorageClient
	scrapers      []*Scraper
	scraperChan   chan *Scraper
	mu            sync.Mutex
}

func NewCoordinator(numScrapers int, frontierAddress string, storageAddress string) (*Coordinator, error) {
	frontierClient, err := frontier.NewFrontierClient(frontierAddress)
	if err != nil {
		return nil, err
	}

	storageClient, err := storage.NewStorageClient(storageAddress)
	if err != nil {
		return nil, err
	}

	c := &Coordinator{
		scrapers:      make([]*Scraper, numScrapers),
		scraperChan:   make(chan *Scraper, numScrapers),
		urlFrontier:   frontierClient,
		resultsWriter: storageClient,
	}

	for i := 0; i < numScrapers; i++ {
		c.scrapers[i] = NewScraper()
		c.scraperChan <- c.scrapers[i]
	}

	return c, nil
}

func (c *Coordinator) getAvailableScraper() *Scraper {
	select {
	case scraper := <-c.scraperChan:
		return scraper
	case <-time.After(30 * time.Second):
		// if no scraper after 30s, make new one
		c.mu.Lock()
		defer c.mu.Unlock()
		newScraper := NewScraper()
		c.scrapers = append(c.scrapers, newScraper)
		return newScraper
	}
}

func (c *Coordinator) releaseScraper(s *Scraper) {
	c.scraperChan <- s
}

func (c *Coordinator) Run() {
	ctx := context.Background()
	for {
		url, err := c.urlFrontier.GetURL(ctx)
		if err != nil {
			log.Printf("Error reading message from frontier: %v", err)
			continue
		}
		scraper := c.getAvailableScraper()
		go func(url string, scraper *Scraper) {
			result, err := scraper.Scrape(url)
			if err != nil {
				log.Printf("Error scraping article: %v", err)
			} else {
				c.handleResult(ctx, result)
			}
			c.releaseScraper(scraper)
		}(url, scraper)
	}
}

func (c *Coordinator) handleResult(ctx context.Context, result *CrawledPage) {
	// send result to kafka
	resultBytes, err := json.Marshal(result)
	if err != nil {
		log.Printf("Error marshalling result: %v", err)
		return
	}
	err = c.resultsWriter.WriteResult(ctx, resultBytes)
	if err != nil {
		log.Printf("Error writing to results: %v", err)
	}

	// add new urls to frontier
	for _, newURL := range result.ExtractedURLs {
		err = c.urlFrontier.AddURL(ctx, newURL)
		if err != nil {
			log.Printf("Error writing to frontier: %v", err)
		}
	}
}

func convertToScrapeResponse(page *CrawledPage) *pb.ScrapeResponse {
	headers := make(map[string]string)
	for k, v := range *page.Headers {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}

	return &pb.ScrapeResponse{
		Url:           page.URL,
		Timestamp:     timestamppb.New(page.Timestamp),
		StatusCode:    int32(page.StatusCode),
		Html:          page.HTML,
		Title:         page.Title,
		Description:   page.Description,
		ExtractedUrls: page.ExtractedURLs,
		Headers:       headers,
	}
}
