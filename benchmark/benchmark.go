package benchmark

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/Bl4ck-h00d/stashdb/client"
	"github.com/Bl4ck-h00d/stashdb/protobuf"
)

const (
	grpcAddress     = ":9000"
	certificateFile = ""
	commonName      = ""
	defaultBucket   = "benchmark-bucket"
)

// Configuration for stress testing
var testConfig = struct {
	operations  int
	concurrency int
}{
	operations:  10000, // Increase this for stress testing
	concurrency: 100,   // Simulate concurrent clients
}

func Start() {
	// Setup gRPC client
	c, err := client.NewGRPCClientWithContextTLS(context.Background(), grpcAddress, certificateFile, commonName)
	if err != nil {
		slog.Error("failed to create gRPC client", slog.Any("error",err))
	}
	defer c.Close()

	// Create benchmark bucket
	err = createBucket(c, defaultBucket)
	if err != nil {
		slog.Error("Failed to create benchmark bucket", slog.Any("error",err))
	}

	// Run benchmarks
	slog.Info("Starting write benchmark...")
	writeResults := benchmarkWrite(c)

	slog.Info("Starting read benchmark...")
	readResults := benchmarkRead(c)

	// Print Results
	fmt.Printf("\nBenchmark Results:\n")
	fmt.Printf("Write Throughput: %.2f ops/sec\n", writeResults.throughput)
	fmt.Printf("Write Average Latency: %.2f ms\n", writeResults.avgLatency)
	fmt.Printf("Read Throughput: %.2f ops/sec\n", readResults.throughput)
	fmt.Printf("Read Average Latency: %.2f ms\n", readResults.avgLatency)

	// Compare replication impact
	slog.Info("\nRunning replication impact test...")
	testReplicationImpact(c)
}

func createBucket(c *client.GRPCClient, bucket string) error {
	req := &protobuf.CreateBucketRequest{Name: bucket}
	return c.CreateBucket(req)
}

type BenchmarkResults struct {
	throughput float64
	avgLatency float64
}

func benchmarkWrite(c *client.GRPCClient) BenchmarkResults {
	start := time.Now()
	var wg sync.WaitGroup
	latencies := make([]time.Duration, testConfig.operations)
	mu := sync.Mutex{}

	for i := 0; i < testConfig.operations; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			startOp := time.Now()
			req := &protobuf.SetRequest{
				Bucket: defaultBucket,
				Key:    fmt.Sprintf("key-%d", i),
				Value:  fmt.Sprintf("value-%d", i),
			}
			err := c.Set(req)
			if err != nil {
				log.Printf("Failed to set key: %v", err)
			}
			mu.Lock()
			latencies[i] = time.Since(startOp)
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	totalDuration := time.Since(start).Seconds()
	return BenchmarkResults{
		throughput: float64(testConfig.operations) / totalDuration,
		avgLatency: avgLatency(latencies),
	}
}

func benchmarkRead(c *client.GRPCClient) BenchmarkResults {
	start := time.Now()
	var wg sync.WaitGroup
	latencies := make([]time.Duration, testConfig.operations)
	mu := sync.Mutex{}

	for i := 0; i < testConfig.operations; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			startOp := time.Now()
			req := &protobuf.GetRequest{
				Bucket: defaultBucket,
				Key:    fmt.Sprintf("key-%d", i),
			}
			_, err := c.Get(req)
			if err != nil {
				log.Printf("Failed to get key: %v", err)
			}
			mu.Lock()
			latencies[i] = time.Since(startOp)
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	totalDuration := time.Since(start).Seconds()
	return BenchmarkResults{
		throughput: float64(testConfig.operations) / totalDuration,
		avgLatency: avgLatency(latencies),
	}
}

func avgLatency(latencies []time.Duration) float64 {
	var total time.Duration
	for _, latency := range latencies {
		total += latency
	}
	return float64(total.Milliseconds()) / float64(len(latencies))
}

func testReplicationImpact(c *client.GRPCClient) {
	// Write data with Raft enabled
	fmt.Println("Testing with replication (Raft enabled)...")
	withRaftResults := benchmarkWrite(c)

	// Disable Raft replication (requires StashDB configuration support)
	// Restart StashDB with Raft disabled, or use a separate cluster setup here.

	fmt.Println("Testing without replication (Raft disabled)...")
	withoutRaftResults := benchmarkWrite(c)

	// Print comparison
	fmt.Printf("\nReplication Impact:\n")
	fmt.Printf("With Raft - Throughput: %.2f ops/sec, Avg Latency: %.2f ms\n", withRaftResults.throughput, withRaftResults.avgLatency)
	fmt.Printf("Without Raft - Throughput: %.2f ops/sec, Avg Latency: %.2f ms\n", withoutRaftResults.throughput, withoutRaftResults.avgLatency)
}
