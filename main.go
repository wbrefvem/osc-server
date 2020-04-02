package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
)

// Crawlable represents a crawable unit, i.e. domain
type Crawlable struct {
	URL string
}

// CrawlJob represents a queueable crawl
type CrawlJob struct {
	ResponseWriter http.ResponseWriter
	Request        *http.Request
}

var requestChan chan CrawlJob

func processURL(rawURL string) (*url.URL, error) {
	log.Printf("process URL %s\n", rawURL)
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("URL scheme is invalid")
	}

	return parsed, nil
}

func handleCrawlGet(w http.ResponseWriter, r *http.Request) {
	log.Println("Processing GET")
}

func handleCrawlPost(requestChan <-chan CrawlJob, done chan bool) {
	for crawlJob := range requestChan {

		log.Println("Processing POST")
		var c Crawlable

		err := json.NewDecoder(crawlJob.Request.Body).Decode(&c)
		if err != nil {
			log.Printf("ERROR: %s\n", err)
			http.Error(crawlJob.ResponseWriter, "JSON body is malformed", http.StatusBadRequest)
			return
		}

		parsedURL, err := processURL(c.URL)
		if err != nil {
			log.Printf("ERROR: %s\n", err)
			http.Error(crawlJob.ResponseWriter, "Invalid URL", http.StatusBadRequest)
			return
		}

		domainAndPort := strings.Split(parsedURL.Host, ":")
		bareDomain := domainAndPort[0]

		log.Printf("Crawling domain %s\n", bareDomain)

		cmd := exec.Command(
			"scrapy",
			"crawl",
			"osc",
			"-a",
			fmt.Sprintf("allowed_domains=%s,", bareDomain),
			"-a",
			fmt.Sprintf("start_urls=%s,", fmt.Sprintf("%s://%s/%s", parsedURL.Scheme, parsedURL.Host, parsedURL.Path)),
		)

		cmd.Dir = os.Getenv("WORK_DIR")
		if cmd.Dir == "" {
			cmd.Dir = "/opt/crawler"
		}
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		err = cmd.Start()
		if err != nil {
			log.Printf("ERROR: %s\n", err)
			http.Error(crawlJob.ResponseWriter, "failed to start crawl command", http.StatusInternalServerError)
		}

		io.WriteString(crawlJob.ResponseWriter, fmt.Sprintf("started crawl for domain %s", c.URL))
		done <- true
	}

}

func enqueueRequest(crawlJob CrawlJob, requestChan chan CrawlJob) bool {
	select {
	case requestChan <- crawlJob:
		return true
	default:
		return false
	}
}

func handleCrawl(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		done := make(chan bool)
		go handleCrawlPost(requestChan, done)
		newJob := CrawlJob{
			Request:        r,
			ResponseWriter: w,
		}

		if !enqueueRequest(newJob, requestChan) {
			http.Error(w, "Your crawl cannot be processed at this time. Please try again later.", http.StatusServiceUnavailable)
		}

		// block so the response doesn't get written automatically
		<-done
	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func handleDomains(w http.ResponseWriter, r *http.Request) {
	domain := r.URL.Path[len("/domains/"):]
	fmt.Printf("getting site map for domain %s\n", domain)
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "/opt/data"
	}
	domainFile, err := ioutil.ReadFile(fmt.Sprintf("%s/%s.json", dataDir, domain))
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		http.Error(w, "failed to read domain file", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(domainFile)
}

func main() {
	log.Println("Listening for crawl requests...")

	requestChan = make(chan CrawlJob, 4)

	mux := http.NewServeMux()
	mux.Handle("/crawl", http.HandlerFunc(handleCrawl))
	mux.Handle("/domains/", http.HandlerFunc(handleDomains))
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}
