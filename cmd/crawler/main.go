package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"web-crawler/internal/config"
	"web-crawler/internal/crawler"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	coordinator, err := crawler.NewCoordinator(
		cfg.NumScrapers,
		cfg.KafkaBrokers,
		cfg.KafkaTopic,
		cfg.CassandraAddress,
		cfg.RedisAddress,
	)
	if err != nil {
		log.Fatalf("Failed to create coordinator: %v", err)
	}

	go coordinator.Run()

	for _, url := range cfg.SeedURLs {
		if err := coordinator.AddURL(url); err != nil {
			log.Printf("Failed to add seed URL %s: %v", url, err)
		}
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
}
