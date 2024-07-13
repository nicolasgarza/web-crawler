package crawler

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type Coordinator struct {
	urlFrontierReader *kafka.Reader
	urlFrontierWriter *kafka.Writer
	resultsWriter     *kafka.Writer
	scrapers          []*Scraper
	scraperChan       chan *Scraper
	mu                sync.Mutex
}

func NewCoordinator(numScrapers int, kafkaBrokers []string) *Coordinator {
	c := &Coordinator{
		scrapers:    make([]*Scraper, numScrapers),
		scraperChan: make(chan *Scraper, numScrapers),
		urlFrontierReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:   kafkaBrokers,
			Topic:     "url_frontier",
			Partition: 0,
			MinBytes:  10e3, // 10KB
			MaxBytes:  10e6, // 10MB
		}),
		urlFrontierWriter: &kafka.Writer{
			Addr:     kafka.TCP(kafkaBrokers...),
			Topic:    "url_frontier",
			Balancer: &kafka.LeastBytes{},
		},
		resultsWriter: &kafka.Writer{
			Addr:     kafka.TCP(kafkaBrokers...),
			Topic:    "crawl_results",
			Balancer: &kafka.LeastBytes{},
		},
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
		m, err := c.urlFrontierReader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message from frontier: %v", err)
			continue
		}
		url := string(m.Value)
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
	err = c.resultsWriter.WriteMessages(ctx, kafka.Message{
		Value: resultBytes,
	})
	if err != nil {
		log.Printf("Error writing to results: %v", err)
	}

	// add new urls to frontier
	for _, newURL := range result.ExtractedURLs {
		err = c.urlFrontierWriter.WriteMessages(ctx, kafka.Message{
			Value: []byte(newURL),
		})
		if err != nil {
			log.Printf("Error writing to frontier: %v", err)
		}
	}
}
