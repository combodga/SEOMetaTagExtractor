package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/html"
)

type Site struct {
	URL string `json:"url"`
}

type Response struct {
	URL      string            `json:"url"`
	MetaTags map[string]string `json:"meta_tags"`
}

var (
	db          *sql.DB
	redisClient *redis.Client
	ctx         = context.Background()

	// Prometheus metrics
	totalRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"path"},
	)
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"path"},
	)
)

func init() {
	// Register metrics
	prometheus.MustRegister(totalRequests)
	prometheus.MustRegister(requestDuration)
}

func initDB() *sql.DB {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASS")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	dbname := os.Getenv("DB_NAME")

	connStr := fmt.Sprintf("user=%s dbname=%s password=%s host=%s port=%s sslmode=disable",
		user, dbname, password, host, port)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	if err = db.Ping(); err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	return db
}

func initRedis() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}

	return rdb
}

func saveMetaTags(url string, metaTags map[string]string) error {
	metaTagsJSON, err := json.Marshal(metaTags)
	if err != nil {
		return fmt.Errorf("error marshalling meta tags to JSON: %w", err)
	}

	_, err = db.Exec("INSERT INTO site_meta_tags (url, meta_tags) VALUES ($1, $2) ON CONFLICT (url) DO UPDATE SET meta_tags = EXCLUDED.meta_tags", url, metaTagsJSON)
	if err != nil {
		return fmt.Errorf("error executing query to save meta tags: %w", err)
	}

	err = redisClient.Set(ctx, url, metaTagsJSON, 1*time.Hour).Err()
	if err != nil {
		return fmt.Errorf("error saving meta tags to Redis: %w", err)
	}

	return nil
}

func getMetaTagsFromCache(url string) (map[string]string, error) {
	metaTagsJSON, err := redisClient.Get(ctx, url).Result()
	if err == redis.Nil {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("error getting data from Redis: %w", err)
	}

	var metaTags map[string]string
	if err := json.Unmarshal([]byte(metaTagsJSON), &metaTags); err != nil {
		return nil, fmt.Errorf("error unmarshalling data from Redis: %w", err)
	}

	return metaTags, nil
}

func FetchMetaTags(url string) (map[string]string, error) {
	cachedMetaTags, err := getMetaTagsFromCache(url)
	if err != nil {
		return nil, err
	}
	if cachedMetaTags != nil {
		return cachedMetaTags, nil
	}

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("error fetching url: %w", err)
	}
	defer resp.Body.Close()

	doc, err := html.Parse(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error parsing HTML: %w", err)
	}

	metaTags := make(map[string]string)
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "meta" {
			var name, content string
			for _, attr := range n.Attr {
				if attr.Key == "name" {
					name = attr.Val
				} else if attr.Key == "content" {
					content = attr.Val
				}
			}
			if name != "" && content != "" {
				metaTags[name] = content
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)

	return metaTags, nil
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	// Start measuring the duration of the request
	timer := prometheus.NewTimer(requestDuration.WithLabelValues(r.URL.Path))
	defer timer.ObserveDuration()

	totalRequests.WithLabelValues(r.URL.Path).Inc()

	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is supported", http.StatusMethodNotAllowed)
		return
	}

	var sites []Site
	if err := json.NewDecoder(r.Body).Decode(&sites); err != nil {
		http.Error(w, "Invalid input: unable to decode JSON", http.StatusBadRequest)
		return
	}

	var wg sync.WaitGroup
	responses := make([]Response, 0, len(sites))
	responseChan := make(chan Response, len(sites))
	errorChan := make(chan error, len(sites))

	for _, site := range sites {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			tags, err := FetchMetaTags(url)
			if err != nil {
				errorChan <- fmt.Errorf("failed to fetch meta tags for %s: %w", url, err)
				return
			}

			err = saveMetaTags(url, tags)
			if err != nil {
				errorChan <- fmt.Errorf("failed to save meta tags for %s: %w", url, err)
				return
			}

			responseChan <- Response{URL: url, MetaTags: tags}
		}(site.URL)
	}

	go func() {
		wg.Wait()
		close(responseChan)
		close(errorChan)
	}()

	for response := range responseChan {
		responses = append(responses, response)
	}

	for err := range errorChan {
		log.Println(err)
	}

	w.Header().Set("Content-Type", "application/json")
	if len(responses) == 0 {
		http.Error(w, "Failed to process all requests", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(responses)
}

func main() {
	logFile, err := os.OpenFile("app.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)

	db = initDB()
	defer db.Close()

	redisClient = initRedis()
	defer redisClient.Close()

	r := mux.NewRouter()
	r.HandleFunc("/metatags", handleRequest).Methods("POST")
	r.Handle("/metrics", promhttp.Handler()) // Expose metrics

	log.Println("Server is running on port 8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}
