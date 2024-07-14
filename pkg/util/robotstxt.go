package util

import (
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/temoto/robotstxt"
)

func GetDomainFromURL(urlString string) (string, error) {
	parsedURL, err := url.Parse(urlString)
	if err != nil {
		return "", err
	}
	return parsedURL.Scheme + "://" + parsedURL.Host, nil
}

func FetchRobotsTXT(domain string) (string, error) {
	resp, err := http.Get(domain + "/robots.txt")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func IsAllowedByRobotsTXT(robotsTXTContent, URL string) bool {
	robots, err := robotstxt.FromString(robotsTXTContent)
	if err != nil {
		// if we cant parse robots.txt, assume its allowed
		return true
	}

	parsedURL, err := url.Parse(URL)
	if err != nil {
		// assume not allowed if we cant parse url
		return false
	}

	return robots.TestAgent(parsedURL.Path, "MyBotAgent")
}
