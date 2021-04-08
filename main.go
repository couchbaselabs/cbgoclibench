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

	var benchRunning uint32 = 1
	var attlSuccess, attlError, attlOps, attlTime uint64
	var wg sync.WaitGroup

	var sendOneGet func()
	sendOneGet = func() {
		stime := time.Now()

		_, err := coll.Get(selectOneKey(docCount), nil)

		etime := time.Now()
		dtimeMs := etime.Sub(stime).Milliseconds()

		atomic.AddUint64(&attlOps, 1)
		atomic.AddUint64(&attlTime, uint64(dtimeMs))
		if err != nil {
			log.Printf("failed to send get %v", err)
			atomic.AddUint64(&attlError, 1)
		} else {
			atomic.AddUint64(&attlSuccess, 1)
		}

		if atomic.LoadUint32(&benchRunning) == 1 {
			sendOneGet()
		} else {
			wg.Done()
		}
	}

	stime := time.Now()

	endWaitCh := time.After(duration)

	for i := 0; i < numConcurrentOps; i++ {
		wg.Add(1)
		go sendOneGet()
	}

	<-endWaitCh
	atomic.StoreUint32(&benchRunning, 0)
	wg.Wait()

	etime := time.Now()
	dtimeMs := etime.Sub(stime).Milliseconds()

	ttlSuccess := atomic.LoadUint64(&attlSuccess)
	ttlError := atomic.LoadUint64(&attlError)
	ttlOps := atomic.LoadUint64(&attlOps)
	ttlTime := atomic.LoadUint64(&attlTime)

	log.Printf("gocb:")
	log.Printf("	Ran for %dms", dtimeMs)
	log.Printf("	Read %d docs", ttlSuccess)
	log.Printf("	Had %d errors", ttlError)
	log.Printf("	Average docs/s of %.2f", float64(ttlSuccess)/float64(dtimeMs)*1000)
	log.Printf("	Average latency of %f milliseconds", float64(ttlTime)/float64(ttlOps))

	cluster.Close(nil)
}

func benchGocbv1(duration time.Duration, addr, username, password string, docCount int, numConcurrentOps int) {
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

	var benchRunning uint32 = 1
	var attlSuccess, attlError, attlOps, attlTime uint64
	var wg sync.WaitGroup

	var sendOneGet func()
	sendOneGet = func() {
		stime := time.Now()

		var b []byte
		_, err := bucket.Get(selectOneKey(docCount), &b)

		etime := time.Now()
		dtimeMs := etime.Sub(stime).Milliseconds()

		atomic.AddUint64(&attlOps, 1)
		atomic.AddUint64(&attlTime, uint64(dtimeMs))
		if err != nil {
			log.Printf("failed to send get %v", err)
			atomic.AddUint64(&attlError, 1)
		} else {
			atomic.AddUint64(&attlSuccess, 1)
		}

		if atomic.LoadUint32(&benchRunning) == 1 {
			sendOneGet()
		} else {
			wg.Done()
		}
	}

	stime := time.Now()

	endWaitCh := time.After(duration)

	for i := 0; i < numConcurrentOps; i++ {
		wg.Add(1)
		go sendOneGet()
	}

	<-endWaitCh
	atomic.StoreUint32(&benchRunning, 0)
	wg.Wait()

	etime := time.Now()
	dtimeMs := etime.Sub(stime).Milliseconds()

	ttlSuccess := atomic.LoadUint64(&attlSuccess)
	ttlError := atomic.LoadUint64(&attlError)
	ttlOps := atomic.LoadUint64(&attlOps)
	ttlTime := atomic.LoadUint64(&attlTime)

	log.Printf("gocbv1:")
	log.Printf("	Ran for %dms", dtimeMs)
	log.Printf("	Read %d docs", ttlSuccess)
	log.Printf("	Had %d errors", ttlError)
	log.Printf("	Average docs/s of %.2f", float64(ttlSuccess)/float64(dtimeMs)*1000)
	log.Printf("	Average latency of %f milliseconds", float64(ttlTime)/float64(ttlOps))

	cluster.Close()
}

func benchGocbcore(duration time.Duration, addr, username, password string, docCount int, kvPoolSize, maxQueueSize, numConcurrentOps int) {
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
	var benchRunning uint32 = 1
	var attlSuccess, attlError, attlOps, attlTime uint64
	var wg sync.WaitGroup

	var sendOneGet func()
	sendOneGet = func() {
		stime := time.Now()
		_, err = agent.Get(gocbcore.GetOptions{
			Key: []byte(selectOneKey(docCount)),
		}, func(result *gocbcore.GetResult, err error) {
			etime := time.Now()
			dtimeMs := etime.Sub(stime).Milliseconds()

			atomic.AddUint64(&attlOps, 1)
			atomic.AddUint64(&attlTime, uint64(dtimeMs))
			if err != nil {
				log.Printf("failed to send get %v", err)
				atomic.AddUint64(&attlError, 1)
			} else {
				atomic.AddUint64(&attlSuccess, 1)
			}

			if atomic.LoadUint32(&benchRunning) == 1 {
				sendOneGet()
			} else {
				wg.Done()
			}
		})
		if err != nil {
			log.Printf("failed to send get %v", err)
			atomic.AddUint64(&attlError, 1)
		}
	}

	stime := time.Now()

	endWaitCh := time.After(duration)

	for i := 0; i < numConcurrentOps; i++ {
		wg.Add(1)
		sendOneGet()
	}

	<-endWaitCh
	atomic.StoreUint32(&benchRunning, 0)
	wg.Wait()

	etime := time.Now()
	dtimeMs := etime.Sub(stime).Milliseconds()

	ttlSuccess := atomic.LoadUint64(&attlSuccess)
	ttlError := atomic.LoadUint64(&attlError)
	ttlOps := atomic.LoadUint64(&attlOps)
	ttlTime := atomic.LoadUint64(&attlTime)

	log.Printf("gocbcore:")
	log.Printf("	Ran for %dms", dtimeMs)
	log.Printf("	Read %d docs", ttlSuccess)
	log.Printf("	Had %d errors", ttlError)
	log.Printf("	Average docs/s of %.2f", float64(ttlSuccess)/float64(dtimeMs)*1000)
	log.Printf("	Average latency of %f milliseconds", float64(ttlTime)/float64(ttlOps))

	agent.Close()
}

func benchGocbcorev7(duration time.Duration, addr, username, password string, docCount int, kvPoolSize, maxQueueSize, numConcurrentOps int) {
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
	var benchRunning uint32 = 1
	var attlSuccess, attlError, attlOps, attlTime uint64
	var wg sync.WaitGroup

	var sendOneGet func()
	sendOneGet = func() {
		stime := time.Now()
		_, err = agent.GetEx(gocbcorev7.GetOptions{
			Key: []byte(selectOneKey(docCount)),
		}, func(result *gocbcorev7.GetResult, err error) {
			etime := time.Now()
			dtimeMs := etime.Sub(stime).Milliseconds()

			atomic.AddUint64(&attlOps, 1)
			atomic.AddUint64(&attlTime, uint64(dtimeMs))
			if err != nil {
				log.Printf("failed to send get %v", err)
				atomic.AddUint64(&attlError, 1)
			} else {
				atomic.AddUint64(&attlSuccess, 1)
			}

			if atomic.LoadUint32(&benchRunning) == 1 {
				sendOneGet()
			} else {
				wg.Done()
			}
		})
		if err != nil {
			log.Printf("failed to send get %v", err)
			atomic.AddUint64(&attlError, 1)
		}
	}

	stime := time.Now()

	endWaitCh := time.After(duration)

	for i := 0; i < numConcurrentOps; i++ {
		wg.Add(1)
		sendOneGet()
	}

	<-endWaitCh
	atomic.StoreUint32(&benchRunning, 0)
	wg.Wait()

	etime := time.Now()
	dtimeMs := etime.Sub(stime).Milliseconds()

	ttlSuccess := atomic.LoadUint64(&attlSuccess)
	ttlError := atomic.LoadUint64(&attlError)
	ttlOps := atomic.LoadUint64(&attlOps)
	ttlTime := atomic.LoadUint64(&attlTime)

	log.Printf("gocbcore:")
	log.Printf("	Ran for %dms", dtimeMs)
	log.Printf("	Read %d docs", ttlSuccess)
	log.Printf("	Had %d errors", ttlError)
	log.Printf("	Average docs/s of %.2f", float64(ttlSuccess)/float64(dtimeMs)*1000)
	log.Printf("	Average latency of %f milliseconds", float64(ttlTime)/float64(ttlOps))

	agent.Close()
}

func benchGocouch(duration time.Duration, addr, username, password string, docCount int, numConcurrentOps int) {
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
	var benchRunning uint32 = 1
	var attlSuccess, attlError, attlOps, attlTime uint64
	var wg sync.WaitGroup

	var sendOneGet func()
	sendOneGet = func() {
		stime := time.Now()

		_, err := bucket.GetRaw(selectOneKey(docCount))

		etime := time.Now()
		dtimeMs := etime.Sub(stime).Milliseconds()

		atomic.AddUint64(&attlOps, 1)
		atomic.AddUint64(&attlTime, uint64(dtimeMs))
		if err != nil {
			log.Printf("failed to send get %v", err)
			atomic.AddUint64(&attlError, 1)
		} else {
			atomic.AddUint64(&attlSuccess, 1)
		}

		if atomic.LoadUint32(&benchRunning) == 1 {
			sendOneGet()
		} else {
			wg.Done()
		}
	}

	stime := time.Now()

	endWaitCh := time.After(duration)

	for i := 0; i < numConcurrentOps; i++ {
		wg.Add(1)
		go sendOneGet()
	}

	<-endWaitCh
	atomic.StoreUint32(&benchRunning, 0)
	wg.Wait()

	etime := time.Now()
	dtimeMs := etime.Sub(stime).Milliseconds()

	ttlSuccess := atomic.LoadUint64(&attlSuccess)
	ttlError := atomic.LoadUint64(&attlError)
	ttlOps := atomic.LoadUint64(&attlOps)
	ttlTime := atomic.LoadUint64(&attlTime)

	log.Printf("gocouch:")
	log.Printf("	Ran for %dms", dtimeMs)
	log.Printf("	Read %d docs", ttlSuccess)
	log.Printf("	Had %d errors", ttlError)
	log.Printf("	Average docs/s of %.2f", float64(ttlSuccess)/float64(dtimeMs)*1000)
	log.Printf("	Average latency of %f milliseconds", float64(ttlTime)/float64(ttlOps))

	bucket.Close()
}

func benchGocouchBulk(duration time.Duration, addr, username, password string, docCount int, numConcurrentBatches, batchSize int) {
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
	var benchRunning uint32 = 1
	var attlSuccess, attlError, attlOps, attlTime uint64
	var wg sync.WaitGroup

	var sendOneGet func()
	sendOneGet = func() {
		batchKeys := make([]string, batchSize)

		stime := time.Now()

		for i := 0; i < batchSize; i++ {
			batchKeys[i] = selectOneKey(docCount)
		}
		res, err := bucket.GetBulkRaw(batchKeys)

		etime := time.Now()
		dtimeMs := etime.Sub(stime).Milliseconds()

		// This is needed due to deduplication
		numFetchedDocs := len(res)

		atomic.AddUint64(&attlOps, 1)
		atomic.AddUint64(&attlTime, uint64(dtimeMs))
		if err != nil {
			log.Printf("failed to send bulk get %v", err)
			atomic.AddUint64(&attlError, uint64(numFetchedDocs))
		} else {
			atomic.AddUint64(&attlSuccess, uint64(numFetchedDocs))
		}

		if atomic.LoadUint32(&benchRunning) == 1 {
			sendOneGet()
		} else {
			wg.Done()
		}
	}

	stime := time.Now()

	endWaitCh := time.After(duration)

	for i := 0; i < numConcurrentBatches; i++ {
		wg.Add(1)
		go sendOneGet()
	}

	<-endWaitCh
	atomic.StoreUint32(&benchRunning, 0)
	wg.Wait()

	etime := time.Now()
	dtimeMs := etime.Sub(stime).Milliseconds()

	ttlSuccess := atomic.LoadUint64(&attlSuccess)
	ttlError := atomic.LoadUint64(&attlError)
	ttlOps := atomic.LoadUint64(&attlOps)
	ttlTime := atomic.LoadUint64(&attlTime)

	log.Printf("gocouchbulk:")
	log.Printf("	Ran for %dms", dtimeMs)
	log.Printf("	Read %d docs", ttlSuccess)
	log.Printf("	Had %d errors", ttlError)
	log.Printf("	Average docs/s of %.2f", float64(ttlSuccess)/float64(dtimeMs)*1000)
	log.Printf("	Average latency of %f milliseconds", float64(ttlTime)/float64(ttlOps))

	bucket.Close()
}
