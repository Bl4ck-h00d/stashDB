package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"log/slog"

	"github.com/Bl4ck-h00d/stashdb/client"
	"github.com/Bl4ck-h00d/stashdb/protobuf"
)

// UserProfile defines different types of users and their behavior patterns
type UserProfile struct {
	readPercentage  int           // Percentage of read operations
	writePercentage int           // Percentage of write operations
	batchSize       int           // Number of operations per batch
	thinkTime       time.Duration // Delay between operations
}

// StressTestConfig holds the configuration for stress testing
type StressTestConfig struct {
	numUsers        int           // Number of concurrent users
	testDuration    time.Duration // How long to run the test
	rampUpTime      time.Duration // Time to gradually add users
	profiles        []UserProfile // Different user profiles to simulate
	monitorInterval time.Duration // Interval for printing metrics
}

// StressTestResults contains the results of the stress test
type StressTestResults struct {
	totalOperations int64
	readOps         int64
	writeOps        int64
	errors          int64
	avgLatency      float64 // Average read latency
	p95Latency      float64 // P95 read latency
	p99Latency      float64 // P99 read latency
	avgWriteLatency float64 // Average write latency
	p95WriteLatency float64 // P95 write latency
	p99WriteLatency float64 // P99 write latency
}

func RunStressTest(c *client.GRPCClient) {
	const bucketName = "defaultBucket"

	// Create bucket
	if err := createBucket(c, bucketName); err != nil {
		slog.Error("Failed to create bucket", slog.String("bucket", bucketName), slog.Any("error", err))
		return
	}
	config := StressTestConfig{
		numUsers:        1000,
		testDuration:    8 * time.Minute,
		rampUpTime:      30 * time.Second,
		monitorInterval: 5 * time.Second,
		profiles: []UserProfile{
			{
				readPercentage:  80,
				writePercentage: 20,
				batchSize:       5,
				thinkTime:       500 * time.Millisecond,
			},
			{
				readPercentage:  20,
				writePercentage: 80,
				batchSize:       10,
				thinkTime:       500 * time.Millisecond,
			},
		},
	}

	slog.Info("Starting stress test...",
		slog.Int("users", config.numUsers),
		slog.Duration("duration", config.testDuration))

	ctx, cancel := context.WithTimeout(context.Background(), config.testDuration)
	defer cancel()

	var wg sync.WaitGroup
	metrics := &sync.Map{}

	// Start metrics collector
	go collectMetrics(ctx, metrics, config.monitorInterval)

	// Gradually add users during ramp-up period
	usersPerBatch := config.numUsers / int(config.rampUpTime/time.Second)
	for i := 0; i < config.numUsers; i++ {
		wg.Add(1)
		go func(userID int) {
			defer wg.Done()
			// Select random profile for this user
			profile := config.profiles[rand.Intn(len(config.profiles))]
			simulateUser(ctx, c, userID, profile, metrics)
		}(i)

		if (i+1)%usersPerBatch == 0 {
			time.Sleep(time.Second)
		}
	}

	// Wait for test completion
	wg.Wait()
	results := calculateResults(metrics)
	printResults(results)
}

func simulateUser(ctx context.Context, c *client.GRPCClient, userID int, profile UserProfile, metrics *sync.Map) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Perform batch of operations
			for i := 0; i < profile.batchSize; i++ {
				if rand.Intn(100) < profile.readPercentage {
					performRead(c, userID, metrics)
				} else {
					performWrite(c, userID, metrics)
				}
			}
			time.Sleep(profile.thinkTime)
		}
	}
}

func performRead(c *client.GRPCClient, userID int, metrics *sync.Map) {
	start := time.Now()
	req := &protobuf.GetRequest{
		Bucket: "defaultBucket",
		Key:    fmt.Sprintf("user-%d-key-%d", userID, rand.Intn(1000)),
	}

	_, err := c.Get(req)
	duration := time.Since(start)

	updateMetrics(metrics, "read", duration, err)
}

func performWrite(c *client.GRPCClient, userID int, metrics *sync.Map) {
	start := time.Now()
	req := &protobuf.SetRequest{
		Bucket: "defaultBucket",
		Key:    fmt.Sprintf("user-%d-key-%d", userID, rand.Intn(1000)),
		Value:  fmt.Sprintf("value-%d-%d", userID, time.Now().UnixNano()),
	}

	err := c.Set(req)
	duration := time.Since(start)

	updateMetrics(metrics, "write", duration, err)
}

func updateMetrics(metrics *sync.Map, opType string, duration time.Duration, err error) {
	// Store latency separately for read and write operations
	latencies, _ := metrics.LoadOrStore("latencies", &sync.Map{})
	latencyMap := latencies.(*sync.Map)

	if opType == "read" {
		latencyMap.Store(fmt.Sprintf("read-%d", time.Now().UnixNano()), duration)
	} else if opType == "write" {
		latencyMap.Store(fmt.Sprintf("write-%d", time.Now().UnixNano()), duration)
	}

	// Update operation counts
	if err != nil {
		incrementCounter(metrics, "errors")
	} else {
		incrementCounter(metrics, opType+"_ops")
	}
}

func incrementCounter(metrics *sync.Map, key string) {
	for {
		val, _ := metrics.LoadOrStore(key, int64(0))
		current := val.(int64)
		if metrics.CompareAndSwap(key, current, current+1) {
			break
		}
	}
}

func collectMetrics(ctx context.Context, metrics *sync.Map, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			printCurrentMetrics(metrics)
		}
	}
}

func printCurrentMetrics(metrics *sync.Map) {
	var readOps, writeOps, errors int64
	metrics.Range(func(key, value interface{}) bool {
		switch key.(string) {
		case "read_ops":
			readOps = value.(int64)
		case "write_ops":
			writeOps = value.(int64)
		case "errors":
			errors = value.(int64)
		}
		return true
	})

	slog.Info("Current metrics",
		slog.Int64("reads", readOps),
		slog.Int64("writes", writeOps),
		slog.Int64("errors", errors),
		slog.Float64("error_rate", float64(errors)/float64(readOps+writeOps)*100))
}

func calculateAvgLatency(durations []time.Duration) float64 {
	if len(durations) == 0 {
		return 0
	}

	var total time.Duration
	for _, duration := range durations {
		total += duration
	}

	return float64(total.Milliseconds()) / float64(len(durations))
}

func calculatePercentileLatency(durations []time.Duration, percentile float64) float64 {
	if len(durations) == 0 {
		return 0
	}

	// Create a copy of durations to avoid modifying the original slice
	sortedDurations := make([]time.Duration, len(durations))
	copy(sortedDurations, durations)

	// Sort the durations in ascending order
	sort.Slice(sortedDurations, func(i, j int) bool {
		return sortedDurations[i] < sortedDurations[j]
	})

	// Calculate the index for the percentile
	index := int(float64(len(sortedDurations)-1) * (percentile / 100))

	// Return the latency at the calculated index in milliseconds
	return float64(sortedDurations[index].Milliseconds())
}

func calculateResults(metrics *sync.Map) StressTestResults {
	// Calculate final metrics
	var results StressTestResults

	metrics.Range(func(key, value interface{}) bool {
		switch key.(string) {
		case "read_ops":
			results.readOps = value.(int64)
		case "write_ops":
			results.writeOps = value.(int64)
		case "errors":
			results.errors = value.(int64)
		}
		return true
	})

	results.totalOperations = results.readOps + results.writeOps

	// Calculate latency percentiles for read and write separately
	latencies, _ := metrics.Load("latencies")
	latencyMap := latencies.(*sync.Map)

	var readDurations, writeDurations []time.Duration
	latencyMap.Range(func(key, value interface{}) bool {
		if key.(string)[:4] == "read" {
			readDurations = append(readDurations, value.(time.Duration))
		} else if key.(string)[:5] == "write" {
			writeDurations = append(writeDurations, value.(time.Duration))
		}
		return true
	})

	if len(readDurations) > 0 {
		results.avgLatency = calculateAvgLatency(readDurations)
		results.p95Latency = calculatePercentileLatency(readDurations, 95)
		results.p99Latency = calculatePercentileLatency(readDurations, 99)
	}

	// Store separate latency values for read and write operations
	results.avgWriteLatency = calculateAvgLatency(writeDurations)
	results.p95WriteLatency = calculatePercentileLatency(writeDurations, 95)
	results.p99WriteLatency = calculatePercentileLatency(writeDurations, 99)

	return results
}
func printResults(results StressTestResults) {
	fmt.Printf("\nStress Test Results:\n")
	fmt.Printf("Total Operations: %d\n", results.totalOperations)
	fmt.Printf("Read Operations: %d\n", results.readOps)
	fmt.Printf("Write Operations: %d\n", results.writeOps)
	fmt.Printf("Errors: %d (%.2f%%)\n", results.errors, float64(results.errors)/float64(results.totalOperations)*100)
	fmt.Printf("Average Read Latency: %.2f ms\n", results.avgLatency)
	fmt.Printf("P95 Read Latency: %.2f ms\n", results.p95Latency)
	fmt.Printf("P99 Read Latency: %.2f ms\n", results.p99Latency)
	fmt.Printf("Average Write Latency: %.2f ms\n", results.avgWriteLatency)
	fmt.Printf("P95 Write Latency: %.2f ms\n", results.p95WriteLatency)
	fmt.Printf("P99 Write Latency: %.2f ms\n", results.p99WriteLatency)
}
