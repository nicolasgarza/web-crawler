# Distributed Web Crawler

This project distributed web crawler implemented in Go. It uses Kafka for URL frontier management, Redis for caching, and Cassandra for storing crawled data.


## Quickstart

1. Install Docker on your system.

2. Clone this repository:
git clone https://github.com/yourusername/distributed-web-crawler.git

cd distributed-web-crawler

3. Run the setup script:
./scripts/setup.sh

The setup script will start all necessary Docker containers and launch the crawler. Once you run the script once, you can just start the crawler with "go run cmd/crawler/main.go".

To stop the crawler, press Ctrl+C, and then run `./scripts/cleanup.sh` to stop and remove the Docker containers.
