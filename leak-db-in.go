package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/schollz/progressbar/v3"
)

const (
	elasticsearchURL      = "https://localhost:9200"
	elasticsearchUser     = "elastic"
	elasticsearchPassword = "changeme"
	logsDir               = "logs"
	maxThreads            = 10
	chunkSize             = 1000
	logInfo               = "info"
	logError              = "error"
)

var (
	esClient    *elastic.Client
	infoLogger  *log.Logger
	errorLogger *log.Logger
	wg          sync.WaitGroup
)

func initElasticsearch() error {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		DialContext: (&net.Dialer{
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: maxThreads,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{
		Transport: tr,
	}

	var err error
	esClient, err = elastic.NewClient(
		elastic.SetURL(elasticsearchURL),
		elastic.SetBasicAuth(elasticsearchUser, elasticsearchPassword),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
		elastic.SetScheme("https"),
		elastic.SetHttpClient(client),
	)
	return err
}

func createIndex(indexName string, properties map[string]interface{}) error {
	exists, err := esClient.IndexExists(indexName).Do(context.Background())
	if err != nil {
		return err
	}

	if !exists {
		_, err = esClient.CreateIndex(indexName).BodyJson(map[string]interface{}{
			"mappings": map[string]interface{}{
				"properties": properties,
			},
		}).Do(context.Background())
	}
	return err
}

func verifyFile(filePath string) error {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return fmt.Errorf("file '%s' not found", filePath)
	}
	return nil
}

func calculateHash(data string) string {
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

func initLoggers() error {
	os.MkdirAll(logsDir, os.ModePerm)

	infoLogFile, err := os.OpenFile(fmt.Sprintf("%s/script.log", logsDir), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}

	errorLogFile, err := os.OpenFile(fmt.Sprintf("%s/error.log", logsDir), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}

	infoLogger = log.New(infoLogFile, "", log.LstdFlags)
	errorLogger = log.New(errorLogFile, "", log.LstdFlags)

	return nil
}

func logMessage(message, level string) {
	timestamp := time.Now().Format(time.RFC3339)
	logEntry := fmt.Sprintf("%s - %s - %s", timestamp, strings.ToUpper(level), message)

	if level == logError {
		errorLogger.Println(logEntry)
	} else {
		infoLogger.Println(logEntry)
	}
}

func entryExists(indexName, hashValue string) (bool, error) {
	query := elastic.NewMatchQuery("hash", hashValue)
	searchResult, err := esClient.Search().
		Index(indexName).
		Query(query).
		Do(context.Background())
	if err != nil {
		return false, err
	}
	return searchResult.TotalHits() > 0, nil
}

func insertNewEntry(indexName, timestamp, hashValue, user, password, url, tag string) error {
	entry := map[string]interface{}{
		"timestamp": timestamp,
		"hash":      hashValue,
		"user":      user,
		"pass":      password,
		"url":       url,
		"tag":       tag,
	}
	_, err := esClient.Index().
		Index(indexName).
		BodyJson(entry).
		Do(context.Background())
	return err
}

func processBatch(lines []string, indexName, delimiter, tag string, combolist bool) {
	defer wg.Done()

	bulkRequest := esClient.Bulk()
	timestamp := time.Now().Format(time.RFC3339)

	for _, line := range lines {
		fields := strings.Split(strings.TrimSpace(line), delimiter)

		if combolist && len(fields) == 2 {
			user, password := fields[0], fields[1]
			hashValue := calculateHash(user + password)
			exists, err := entryExists(indexName, hashValue)
			if err != nil {
				logMessage(fmt.Sprintf("Error checking entry existence: %v", err), logError)
				continue
			}
			if !exists {
				entry := map[string]interface{}{
					"timestamp": timestamp,
					"hash":      hashValue,
					"user":      user,
					"pass":      password,
					"tag":       tag,
				}
				req := elastic.NewBulkIndexRequest().Index(indexName).Doc(entry)
				bulkRequest = bulkRequest.Add(req)
			}
		} else if !combolist && len(fields) == 3 {
			url, user, password := fields[0], fields[1], fields[2]
			hashValue := calculateHash(url + user + password)
			exists, err := entryExists(indexName, hashValue)
			if err != nil {
				logMessage(fmt.Sprintf("Error checking entry existence: %v", err), logError)
				continue
			}
			if !exists {
				entry := map[string]interface{}{
					"timestamp": timestamp,
					"hash":      hashValue,
					"url":       url,
					"user":      user,
					"pass":      password,
					"tag":       tag,
				}
				req := elastic.NewBulkIndexRequest().Index(indexName).Doc(entry)
				bulkRequest = bulkRequest.Add(req)
			}
		} else {
			logMessage(fmt.Sprintf("Invalid input: %s", line), logError)
		}
	}

	if bulkRequest.NumberOfActions() > 0 {
		_, err := bulkRequest.Do(context.Background())
		if err != nil {
			logMessage(fmt.Sprintf("Error inserting bulk entries: %v", err), logError)
		}
	}
}

func worker(lines <-chan []string, indexName, delimiter, tag string, combolist bool) {
	for batch := range lines {
		wg.Add(1)
		processBatch(batch, indexName, delimiter, tag, combolist)
	}
}

func countLines(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	totalLines := 0
	for {
		_, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		totalLines++
	}
	return totalLines, nil
}

func main() {
	var combolist, infostealer bool
	var filePath, tag string

	flag.BoolVar(&combolist, "combolist", false, "Process combolist file")
	flag.BoolVar(&infostealer, "infostealer", false, "Process infostealer file")
	flag.StringVar(&filePath, "file", "", "Path to the input file")
	flag.StringVar(&tag, "TAG", "", "Tag to identify the source of the imports")
	flag.Parse()

	fmt.Println("Starting script...")

	if !combolist && !infostealer {
		log.Fatal("One of --combolist or --infostealer must be specified")
	}
	if filePath == "" || tag == "" {
		log.Fatal("File path and tag are required")
	}

	// Get current date
	today := time.Now().Format("02-01-2006")

	var indexName string
	var properties map[string]interface{}
	var delimiter string

	if combolist {
		indexName = fmt.Sprintf("combolists-leaks-%s", today)
		properties = map[string]interface{}{
			"timestamp": map[string]string{"type": "date", "format": "strict_date_optional_time||epoch_second"},
			"hash":      map[string]string{"type": "keyword"},
			"user":      map[string]string{"type": "text"},
			"pass":      map[string]string{"type": "text"},
			"tag":       map[string]string{"type": "text"},
		}
		delimiter = ":"
	} else if infostealer {
		indexName = fmt.Sprintf("infostealer-leaks-%s", today)
		properties = map[string]interface{}{
			"timestamp": map[string]string{"type": "date", "format": "strict_date_optional_time||epoch_second"},
			"hash":      map[string]string{"type": "keyword"},
			"url":       map[string]string{"type": "text"},
			"user":      map[string]string{"type": "text"},
			"pass":      map[string]string{"type": "text"},
			"tag":       map[string]string{"type": "text"},
		}
		delimiter = ","
	}

	if err := initLoggers(); err != nil {
		log.Fatalf("Failed to initialize loggers: %v", err)
	}

	logMessage("=============Script started=============", logInfo)
	logMessage(fmt.Sprintf("Index: %s", indexName), logInfo)
	logMessage(fmt.Sprintf("Tag: %s", tag), logInfo)

	err := verifyFile(filePath)
	if err != nil {
		logMessage(fmt.Sprintf("File verification failed for '%s': %v", filePath, err), logError)
		return
	}

	logMessage("Initializing Elasticsearch...", logInfo)
	err = initElasticsearch()
	if err != nil {
		logMessage(fmt.Sprintf("Failed to initialize Elasticsearch: %v", err), logError)
		return
	}

	logMessage("Creating index...", logInfo)
	err = createIndex(indexName, properties)
	if err != nil {
		logMessage(fmt.Sprintf("Failed to create index: %v", err), logError)
		return
	}

	// Count the total number of lines for the progress bar
	totalLines, err := countLines(filePath)
	if err != nil {
		logMessage(fmt.Sprintf("Error counting lines in file: %v", err), logError)
		return
	}

	file, err := os.Open(filePath)
	if err != nil {
		logMessage(fmt.Sprintf("Failed to open file: %v", err), logError)
		return
	}
	defer file.Close()

	bar := progressbar.Default(int64(totalLines))
	lines := make(chan []string, chunkSize)

	// Start worker pool
	for i := 0; i < maxThreads; i++ {
		go worker(lines, indexName, delimiter, tag, combolist)
	}

	var batch []string
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				if len(batch) > 0 {
					lines <- batch
				}
				break
			}
			logMessage(fmt.Sprintf("Error reading file: %v", err), logError)
			return
		}
		batch = append(batch, line)
		if len(batch) >= chunkSize {
			lines <- batch
			batch = nil
		}
		bar.Add(1)
	}

	close(lines)
	wg.Wait()

	logMessage("=============Script finished=============", logInfo)
	fmt.Println("Script finished successfully.")
}
