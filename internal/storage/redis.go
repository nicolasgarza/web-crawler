package storage

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisCache struct {
	client *redis.Client
}

func NewRedisCache(address string) (*RedisCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr: address,
	})

	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		return nil, err
	}

	return &RedisCache{
		client: client,
	}, nil
}

func (rc *RedisCache) SetCrawledURL(ctx context.Context, url string) error {
	return rc.client.Set(ctx, "crawled:"+url, "1", 24*time.Hour).Err()
}

func (rc *RedisCache) HasCrawledURL(ctx context.Context, url string) (bool, error) {
	exists, err := rc.client.Exists(ctx, "crawled:"+url).Result()
	return exists == 1, err
}

func (rc *RedisCache) SetRobotsTXT(ctx context.Context, domain string, content string) (string, error) {
	return rc.client.Set(ctx, "robots:"+domain, content, 24*time.Hour).Result()
}

func (rc *RedisCache) GetRobotsTXT(ctx context.Context, domain string) (string, error) {
	key := "robots:" + domain
	content, err := rc.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	} else if err != nil {
		return "", err
	}
	return content, nil
}

func (rc *RedisCache) Close() error {
	return rc.client.Close()
}
