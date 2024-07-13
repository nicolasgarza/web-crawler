package crawler

import (
	"context"
	"log"
	"sync"
	"time"
	"web-crawler/internal/frontier"
	"web-crawler/internal/storage"
	"web-crawler/pkg/models"
	"web-crawler/pkg/util"

	"github.com/go-redis/redis"
)

type Coordinator struct {
	urlFrontier *frontier.URLFrontier
	storage     *storage.CassandraStorage
	redisCache  *storage.RedisCache
	scrapers    []*Scraper
	scraperChan chan *Scraper
	mu          sync.Mutex
}

func NewCoordinator(numScrapers int, kafkaBrokers []string, kafkaTopic string, storageAddress string, redisAddress string) (*Coordinator, error) {
	frontierClient := frontier.NewURLFrontier(kafkaBrokers, kafkaTopic)

	storageClient, err := storage.NewCassandraStorage([]string{"localhost"}, "crawler_keyspace")
	if err != nil {
		return nil, err
	}

	redisCache, err := storage.NewRedisCache(redisAddress)
	if err != nil {
		return nil, err
	}

	c := &Coordinator{
		scrapers:    make([]*Scraper, numScrapers),
		scraperChan: make(chan *Scraper, numScrapers),
		urlFrontier: frontierClient,
		storage:     storageClient,
		redisCache:  redisCache,
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

		crawled, err := c.redisCache.HasCrawledURL(ctx, url)
		if err != nil {
			log.Printf("Error checking Redis cache: %v", err)
		} else if crawled {
			log.Printf("URL already crawled: %s", url)
			continue
		}

		allowed, err := c.checkRobotsTXT(ctx, url)
		if err != nil {
			log.Printf("Error checking robots.txt: %v", err)
			continue
		}
		if !allowed {
			log.Printf("URL not allowed by robots.txt: %s", url)
			continue
		}

		scraper := c.getAvailableScraper()
		go func(url string, scraper *Scraper) {
			result, err := scraper.Scrape(url)
			if err != nil {
				log.Printf("Error scraping article: %v", err)
			} else {
				c.handleResult(ctx, result)
				if err := c.redisCache.SetCrawledURL(ctx, url); err != nil {
					log.Printf("Error setting crawled URL in Redis: %v", err)
				}
			}
			c.releaseScraper(scraper)
		}(url, scraper)
	}
}

func (c *Coordinator) handleResult(ctx context.Context, result *models.CrawledPage) {
	err := c.storage.StoreCrawledPage(ctx, result)
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

func (c *Coordinator) checkRobotsTXT(ctx context.Context, url string) (bool, error) {
	domain, err := util.GetDomainFromURL(url)
	if err != nil {
		return false, err
	}

	robotsTXT, err := c.redisCache.GetRobotsTXT(ctx, domain)
	if err == redis.Nil {
		// robots.txt not in cache, fetch it
		robotsTXT, err = util.FetchRobotsTXT(domain)
		if err != nil {
			return false, err
		}

		// cache the robots.txt
		if _, err := c.redisCache.SetRobotsTXT(ctx, domain, robotsTXT); err != nil {
			log.Printf("Error caching robots.txt: %v", err)
		}
	} else if err != nil {
		return false, err
	}
	return util.IsAllowedByRobotsTXT(robotsTXT, url), nil
}

func (c *Coordinator) AddURL(url string) error {
	return c.urlFrontier.AddURL(context.Background(), url)
}
