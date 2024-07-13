package crawler

import (
	"time"
	"web-crawler/pkg/models"

	"github.com/gocolly/colly"
)

type Scraper struct {
	c *colly.Collector
}

func NewScraper() *Scraper {
	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"),
	)
	c.AllowURLRevisit = true

	c.Limit(&colly.LimitRule{
		RandomDelay: time.Millisecond * 50,
	})

	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
		r.Headers.Set("Accept-Language", "en-US,en;q=0.5")
		r.Headers.Set("Accept-Encoding", "gzip, deflate, br")
		r.Headers.Set("DNT", "1")
		r.Headers.Set("Connection", "keep-alive")
		r.Headers.Set("Upgrade-Insecure-Requests", "1")
	})

	return &Scraper{
		c: c,
	}
}

func (s *Scraper) Scrape(url string) (*models.CrawledPage, error) {
	page := &models.CrawledPage{URL: url, Timestamp: time.Now()}
	s.c.OnResponse(func(r *colly.Response) {
		page.StatusCode = r.StatusCode
		page.Headers = r.Headers
		page.HTML = string(r.Body)
	})

	s.c.OnHTML("title", func(e *colly.HTMLElement) {
		page.Title = e.Text
	})

	s.c.OnHTML("meta[name=description]", func(e *colly.HTMLElement) {
		page.Description = e.Attr("content")
	})

	s.c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		link := e.Request.AbsoluteURL(e.Attr("href"))
		if link != "" {
			page.ExtractedURLs = append(page.ExtractedURLs, link)
		}
	})

	err := s.c.Visit(url)
	if err != nil {
		return nil, err
	}
	return page, nil
}
