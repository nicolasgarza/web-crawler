package crawler

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"
	"web-crawler/internal/frontier"
)

type Coordinator struct {
	urlFrontier   *frontier.URLFrontier
	resultsWriter *frontier.ResultsWriter
	scrapers      []*Scraper
	scraperChan   chan *Scraper
	mu            sync.Mutex
}

func NewCoordinator(numScrapers int, kafkaBrokers []string) *Coordinator {
	c := &Coordinator{
		scrapers:      make([]*Scraper, numScrapers),
		scraperChan:   make(chan *Scraper, numScrapers),
		urlFrontier:   frontier.NewURLFrontier(kafkaBrokers, "url_frontier"),
		resultsWriter: frontier.NewResultsWriter(kafkaBrokers, "crawl_results"),
	}

	for i := 0; i < numScrapers; i++ {
		c.scrapers[i] = NewScraper()
		c.scraperChan <- c.scrapers[i]
	}

	return c
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
