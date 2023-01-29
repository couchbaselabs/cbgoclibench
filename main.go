package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/go-couchbase"
	gocbv1 "github.com/couchbase/gocb"
	"github.com/couchbase/gocb/v2"
	gocbcorev7 "github.com/couchbase/gocbcore"
	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/goutils/logging"
)

func genTestData(addr, user, pass, bucketName string, docCount int) {
	connStr := fmt.Sprintf("couchbase://%s", addr)
	cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
		Transcoder: gocb.NewRawBinaryTranscoder(),
	})
	if err != nil {
		panic(err)
	}

	bucket := cluster.Bucket("default")
	col := bucket.DefaultCollection()

	// Generate random bytes for the document contents
	docSize := 1024
	docVariants := 1024
	randomBytes := make([]byte, docSize*docVariants)
	for i := 0; i < len(randomBytes); i++ {
		randomBytes[i] = byte(i)
	}

	batchSize := 2000
	batchCount := (docCount + batchSize - 1) / batchSize

	for j := 0; j < batchCount; j++ {
		var wg sync.WaitGroup
		for i := 0; i < batchSize; i++ {
			wg.Add(1)
			go func(id int) {
				if id >= docCount {
					wg.Done()
					return
				}

				maxStart := len(randomBytes) - docSize
				myStart := rand.Intn(maxStart)
				myBytes := randomBytes[myStart : myStart+docSize]

				_, err = col.Upsert(fmt.Sprintf("upsert-get-%d", id), myBytes, &gocb.UpsertOptions{
					Timeout: 10 * time.Second,
				})
				if err != nil {
					log.Printf("failed to upsert test data %v", err)
				}
				wg.Done()
			}(i + (batchSize * j))
		}
		wg.Wait()
	}
}

func selectOneKey(docCount int) string {
	docID := rand.Intn(docCount)
	return fmt.Sprintf("upsert-get-%d", docID)
}

type benchRunner struct {
	Name string

	startTime  time.Time
	isRunning  uint64
	ttlTimeNs  uint64
	ttlSuccess uint64
	ttlError   uint64
}

type benchRunnerOp struct {
	startTime time.Time
}

func (b *benchRunner) Start() {
	atomic.StoreUint64(&b.isRunning, 1)
	b.startTime = time.Now()
	b.ttlTimeNs = 0
	b.ttlSuccess = 0
	b.ttlError = 0
}

func (b *benchRunner) WaitAndStop(t time.Duration) {
	<-time.After(time.Until(b.startTime.Add(t)))
	atomic.StoreUint64(&b.isRunning, 0)
}

func (b *benchRunner) EndAndPrintReport() {
	dtime := time.Since(b.startTime)
	dtimeMs := dtime.Milliseconds()

	ttlSuccess := atomic.LoadUint64(&b.ttlSuccess)
	ttlError := atomic.LoadUint64(&b.ttlError)
	ttlOps := ttlSuccess + ttlError
	ttlTimeNs := atomic.LoadUint64(&b.ttlTimeNs)

	ttlTime := time.Duration(ttlTimeNs) * time.Nanosecond

	log.Printf("%s:", b.Name)
	log.Printf("	Ran for %dms", dtimeMs)
	log.Printf("	Read %d docs (+%d errors)", ttlSuccess, ttlError)
	log.Printf("	Average docs/s of %.0f", float64(ttlSuccess)/float64(dtime/time.Second))
	log.Printf("	Average latency of %f milliseconds", float64(ttlTime/time.Millisecond)/float64(ttlOps))
}

func (b *benchRunner) StartOp() benchRunnerOp {
	return benchRunnerOp{
		startTime: time.Now(),
	}
}

func (b *benchRunner) EndMultiOp(o benchRunnerOp, err error, numOps uint64) {
	dtime := time.Since(o.startTime)
	dtimeNs := uint64(dtime / time.Nanosecond)

	if err == nil {
		atomic.AddUint64(&b.ttlSuccess, numOps)
	} else {
		atomic.AddUint64(&b.ttlError, numOps)
	}
	atomic.AddUint64(&b.ttlTimeNs, dtimeNs*numOps)
}

func (b *benchRunner) EndOp(o benchRunnerOp, err error) {
	b.EndMultiOp(o, err, 1)
}

func (b *benchRunner) IsRunning() bool {
	return atomic.LoadUint64(&b.isRunning) != 0
}

func main() {
	addr := flag.String("host", "172.23.111.135", "The address to connect to")
	username := flag.String("username", "Administrator", "The username to use")
	password := flag.String("password", "password", "The password to use")
	bucket := flag.String("bucket", "default", "The bucket to use")
	duration := flag.Duration("duration", 5*time.Second, "Duration to fetch docs for")
	noGenData := flag.Bool("no-gen-data", false, "Whether to skip generating data before running gets")
	docCount := flag.Int("doc-count", 1024, "Number of documents used for testing")

	numConcurrentOps := flag.Int("num-concurrent-ops", 2048, "number of concurrent ops")
	maxQueueSize := flag.Int("max-queue-size", 2048, "bounded queue size within the client")
	connPoolSize := flag.Int("conn-pool-size", 1, "connection pool size")
	batchSize := flag.Int("batch-size", 512, "batch size")

	flag.Parse()

	f, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	if flag.NArg() < 1 {
		panic("must specify a test to run (gocb, gocbcore, gocouchbase)")
	}

	testName := flag.Arg(0)

	if !(*noGenData) {
		log.Printf("Inserting Test Data...")
		genTestData(*addr, *username, *password, *bucket, *docCount)
		log.Printf("Test Data Inserted")
	}

	switch testName {
	case "gocb":
		log.Printf("Running `gocb` benchmark...")
		benchGocb(*duration, *addr, *username, *password, *docCount, *numConcurrentOps)
	case "gocbv1":
		log.Printf("Running `gocbv1` benchmark...")
		benchGocbv1(*duration, *addr, *username, *password, *docCount, *numConcurrentOps)
	case "gocbcore":
		log.Printf("Running `gocbcore` benchmark...")
		benchGocbcore(*duration, *addr, *username, *password, *docCount, *connPoolSize, *maxQueueSize, *numConcurrentOps)
	case "gocbcorev7":
		log.Printf("Running `gocbcorev7` benchmark...")
		benchGocbcorev7(*duration, *addr, *username, *password, *docCount, *connPoolSize, *maxQueueSize, *numConcurrentOps)
	case "gocouch":
		log.Printf("Running `gocouch` benchmark...")
		benchGocouch(*duration, *addr, *username, *password, *docCount, *numConcurrentOps)
	case "gocouchbulk":
		log.Printf("Running `gocouchbulk` benchmark...")
		benchGocouchBulk(*duration, *addr, *username, *password, *docCount, *numConcurrentOps, *batchSize)
	}
}

func benchGocb(duration time.Duration, addr, username, password string, docCount int, numConcurrentOps int) {
	bench := benchRunner{
		Name: fmt.Sprintf("Gocb (docs: %d, concurrency: %d)",
			docCount, numConcurrentOps)}

	connStr := fmt.Sprintf("couchbase://%s", addr)
	cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
		Transcoder: gocb.NewRawBinaryTranscoder(),
	})
	if err != nil {
		panic(err)
	}

	bucket := cluster.Bucket("default")
	coll := bucket.DefaultCollection()

	// Warm up by calling wait until ready
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		panic(err)
	}

	bench.Start()

	var wg sync.WaitGroup
	var sendOneGet func()
	sendOneGet = func() {
		bop := bench.StartOp()

		_, err := coll.Get(selectOneKey(docCount), nil)

		bench.EndOp(bop, err)

		if bench.IsRunning() {
			sendOneGet()
		} else {
			wg.Done()
		}
	}

	for i := 0; i < numConcurrentOps; i++ {
		wg.Add(1)
		go sendOneGet()
	}

	bench.WaitAndStop(duration)
	wg.Wait()

	bench.EndAndPrintReport()

	cluster.Close(nil)
}

func benchGocbv1(duration time.Duration, addr, username, password string, docCount int, numConcurrentOps int) {
	bench := benchRunner{
		Name: fmt.Sprintf("Gocbv1 (docs: %d, concurrency: %d)",
			docCount, numConcurrentOps)}

	connStr := fmt.Sprintf("couchbase://%s", addr)
	cluster, err := gocbv1.Connect(connStr)
	if err != nil {
		panic(err)
	}
	cluster.Authenticate(
		gocbv1.PasswordAuthenticator{
			Username: username,
			Password: password,
		})

	bucket, err := cluster.OpenBucket("default", "")
	if err != nil {
		panic(err)
	}

	// Wait until connections are ready by performing a bunch of random operations
	numWarmupOps := 1024
	warmWaitCh := make(chan struct{}, numWarmupOps)
	for i := 0; i < numWarmupOps; i++ {
		go func() {
			var b []byte
			bucket.Get(selectOneKey(docCount), &b)
			warmWaitCh <- struct{}{}
		}()
	}
	for i := 0; i < numWarmupOps; i++ {
		<-warmWaitCh
	}

	bench.Start()

	var wg sync.WaitGroup
	var sendOneGet func()
	sendOneGet = func() {
		bop := bench.StartOp()

		var b []byte
		_, err := bucket.Get(selectOneKey(docCount), &b)

		bench.EndOp(bop, err)

		if bench.IsRunning() {
			sendOneGet()
		} else {
			wg.Done()
		}
	}

	for i := 0; i < numConcurrentOps; i++ {
		wg.Add(1)
		go sendOneGet()
	}

	bench.WaitAndStop(duration)
	wg.Wait()

	bench.EndAndPrintReport()

	cluster.Close()
}

func benchGocbcore(duration time.Duration, addr, username, password string, docCount int, kvPoolSize, maxQueueSize, numConcurrentOps int) {
	bench := benchRunner{
		Name: fmt.Sprintf("Gocbcore (docs: %d, concurrency: %d, pool-size: %d, queue-size: %d)",
			docCount, numConcurrentOps, kvPoolSize, maxQueueSize)}

	connStr := fmt.Sprintf("couchbase://%s", addr)
	config := gocbcore.AgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		panic(err)
	}

	config.KvPoolSize = kvPoolSize
	config.MaxQueueSize = maxQueueSize
	config.Auth = &gocbcore.PasswordAuthProvider{
		Username: username,
		Password: password,
	}
	config.BucketName = "default"

	agent, err := gocbcore.CreateAgent(&config)
	if err != nil {
		panic(err)
	}

	// Warm up by calling wait until ready
	warmWaitCh := make(chan struct{}, 1)
	_, err = agent.WaitUntilReady(
		time.Now().Add(5*time.Second),
		gocbcore.WaitUntilReadyOptions{
			RetryStrategy: gocbcore.NewBestEffortRetryStrategy(nil),
		},
		func(result *gocbcore.WaitUntilReadyResult, err error) {
			if err != nil {
				panic(err)
			}
			warmWaitCh <- struct{}{}
		})
	if err != nil {
		panic(err)
	}
	<-warmWaitCh

	// Do the benchmark
	bench.Start()

	var wg sync.WaitGroup
	var sendOneGet func()
	sendOneGet = func() {
		bop := bench.StartOp()

		_, err = agent.Get(gocbcore.GetOptions{
			Key: []byte(selectOneKey(docCount)),
		}, func(result *gocbcore.GetResult, err error) {
			bench.EndOp(bop, err)

			if bench.IsRunning() {
				sendOneGet()
			} else {
				wg.Done()
			}
		})
		if err != nil {
			bench.EndOp(bop, err)
		}
	}

	for i := 0; i < numConcurrentOps; i++ {
		wg.Add(1)
		sendOneGet()
	}

	bench.WaitAndStop(duration)
	wg.Wait()

	bench.EndAndPrintReport()

	agent.Close()
}

func benchGocbcorev7(duration time.Duration, addr, username, password string, docCount int, kvPoolSize, maxQueueSize, numConcurrentOps int) {
	bench := benchRunner{
		Name: fmt.Sprintf("Gocbcorev7 (docs: %d, concurrency: %d, pool-size: %d, queue-size: %d)",
			docCount, numConcurrentOps, kvPoolSize, maxQueueSize)}

	connStr := fmt.Sprintf("couchbase://%s", addr)
	config := gocbcorev7.AgentConfig{}
	err := config.FromConnStr(connStr)
	if err != nil {
		panic(err)
	}

	config.KvPoolSize = kvPoolSize
	config.MaxQueueSize = maxQueueSize
	config.Auth = &gocbcorev7.PasswordAuthProvider{
		Username: username,
		Password: password,
	}
	config.BucketName = "default"

	agent, err := gocbcorev7.CreateAgent(&config)
	if err != nil {
		panic(err)
	}

	// Wait until connections are ready by performing a bunch of random operations
	numWarmupOps := 1024
	warmWaitCh := make(chan struct{}, numWarmupOps)
	for i := 0; i < numWarmupOps; i++ {
		agent.GetEx(gocbcorev7.GetOptions{
			Key: []byte(selectOneKey(docCount)),
		}, func(result *gocbcorev7.GetResult, err error) {
			warmWaitCh <- struct{}{}
		})
	}
	for i := 0; i < numWarmupOps; i++ {
		<-warmWaitCh
	}

	// Do the benchmark
	bench.Start()

	var wg sync.WaitGroup
	var sendOneGet func()
	sendOneGet = func() {
		bop := bench.StartOp()

		_, err = agent.GetEx(gocbcorev7.GetOptions{
			Key: []byte(selectOneKey(docCount)),
		}, func(result *gocbcorev7.GetResult, err error) {
			bench.EndOp(bop, err)

			if bench.IsRunning() {
				sendOneGet()
			} else {
				wg.Done()
			}
		})
		if err != nil {
			bench.EndOp(bop, err)
		}
	}

	for i := 0; i < numConcurrentOps; i++ {
		wg.Add(1)
		sendOneGet()
	}

	bench.WaitAndStop(duration)
	wg.Wait()

	bench.EndAndPrintReport()

	agent.Close()
}

func benchGocouch(duration time.Duration, addr, username, password string, docCount int, numConcurrentOps int) {
	bench := benchRunner{
		Name: fmt.Sprintf("Gocouch (docs: %d, concurrency: %d)",
			docCount, numConcurrentOps)}

	logging.SetLogger(nil)
	c, err := couchbase.ConnectWithAuthCreds(fmt.Sprintf("http://%s:8091/", addr), username, password)
	if err != nil {
		panic("Error connecting: " + err.Error())
	}

	pool, err := c.GetPool("default")
	if err != nil {
		panic("Error getting pool: " + err.Error())
	}

	bucket, err := pool.GetBucket("default")
	if err != nil {
		panic("Error getting bucket: " + err.Error())
	}

	// Wait until connections are ready by performing a bunch of random operations
	numWarmupOps := 1024
	warmWaitCh := make(chan struct{}, numWarmupOps)
	for i := 0; i < numWarmupOps; i++ {
		go func() {
			bucket.GetRaw(selectOneKey(docCount))
			warmWaitCh <- struct{}{}
		}()
	}
	for i := 0; i < numWarmupOps; i++ {
		<-warmWaitCh
	}

	// Do the benchmark
	bench.Start()

	var wg sync.WaitGroup
	var sendOneGet func()
	sendOneGet = func() {
		bop := bench.StartOp()

		_, err := bucket.GetRaw(selectOneKey(docCount))

		bench.EndOp(bop, err)

		if bench.IsRunning() {
			sendOneGet()
		} else {
			wg.Done()
		}
	}

	for i := 0; i < numConcurrentOps; i++ {
		wg.Add(1)
		go sendOneGet()
	}

	bench.WaitAndStop(duration)
	wg.Wait()

	bench.EndAndPrintReport()

	bucket.Close()
}

func benchGocouchBulk(duration time.Duration, addr, username, password string, docCount int, numConcurrentBatches, batchSize int) {
	bench := benchRunner{
		Name: fmt.Sprintf("GocouchBulk (docs: %d, concurrency: %d, batch-size: %d)",
			docCount, numConcurrentBatches, batchSize)}

	logging.SetLogger(nil)
	c, err := couchbase.ConnectWithAuthCreds(fmt.Sprintf("http://%s:8091/", addr), username, password)
	if err != nil {
		panic("Error connecting: " + err.Error())
	}

	pool, err := c.GetPool("default")
	if err != nil {
		panic("Error getting pool: " + err.Error())
	}

	bucket, err := pool.GetBucket("default")
	if err != nil {
		panic("Error getting bucket: " + err.Error())
	}

	couchbase.InitBulkGet()

	// Wait until connections are ready by performing a bunch of random operations
	numWarmupOps := 1024
	warmWaitCh := make(chan struct{}, numWarmupOps)
	for i := 0; i < numWarmupOps; i++ {
		go func() {
			bucket.GetRaw(selectOneKey(docCount))
			warmWaitCh <- struct{}{}
		}()
	}
	for i := 0; i < numWarmupOps; i++ {
		<-warmWaitCh
	}

	// Do the benchmark
	bench.Start()

	var wg sync.WaitGroup
	var sendOneGet func()
	sendOneGet = func() {
		batchKeys := make([]string, batchSize)

		bop := bench.StartOp()

		for i := 0; i < batchSize; i++ {
			batchKeys[i] = selectOneKey(docCount)
		}
		res, err := bucket.GetBulkRaw(batchKeys)

		// This is needed due to deduplication
		numFetchedDocs := uint64(len(res))

		bench.EndMultiOp(bop, err, numFetchedDocs)

		if bench.IsRunning() {
			sendOneGet()
		} else {
			wg.Done()
		}
	}

	for i := 0; i < numConcurrentBatches; i++ {
		wg.Add(1)
		go sendOneGet()
	}

	bench.WaitAndStop(duration)
	wg.Wait()

	bench.EndAndPrintReport()

	bucket.Close()
}
