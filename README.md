# Couchbase Go Client Benchmarks

## Test Behaviour

This test harness contains four separate benchmarking implementations. Below describes the known
caveats and implementation details of each implementation along with the tunable parameters.

Note: All clients are configured not to perform JSON serialization/deserialization. Either by
configuring their respective clients such that they do not perform transcoding, or by using
specific methods which are intended for raw access and do not deserialize.

Note: This synthetic benchmark is designed to replicate a 100% GET workload. A number of
documents are preloaded by the tests and then randomly selected for each operation which is
performed. Any allocations and time related to this behaviour is not included in bench times.

### gocb

This implementation uses v2 of the gocb client. The test will create `num-concurrent-ops` number
of goroutines, and each goroutine will execute as many serialized GET operations as possible. Upon
test completion, the goroutines will finish any running operations, finalize and the test will end.

### gocbcore

This implementation uses v9 of the gocbcore client (the same as bundled with gocb v2 above). This
test will execute `num-concurrent-ops` asynchronous operations at the beginning of the test. Upon
completion of each op, an additional op will be dispatched such that there are always the same
number of operations pending. Upon test completion, pending ops will be completed and then the
test will end.

### gocouch

This implementation uses the go-couchbase client. This test behaves similar to the gocb test. The
test will create `num-concurrent-ops` number of goroutines, and each goroutine will execute as many
serialized GET operations as possible. Upon test completion, the goroutines will finish any running
operations, finalize and the test will end.

### gocouchbulk

This implementation also uses the go-couchbase client, but instead attempts to take advantage of the
bulk getting capabilities within this client. The test will create `num-concurrent-ops` number of
goroutines. Each goroutine will perform a bulk get for `batch-size` items. Upon test completion,
the goroutines will finish any running operations, finalize and the test will end.

Note: In this test, the average latency will naturally be higher as every `batch-size` group of
documents needs to wait for all the documents contained within that group. As expected, this
increases latency but does not impact the overall throughput (represented as docs/s).

## Test Results

The system under test consisted of a 3 node cluster, running all services. Each system consisted
of 8 VCPUs and 64GB RAM (r5.2xlarge) running on AWS. The client system was identical to one of
the nodes (but did not run on one of the nodes). All systems were within the same AWS region and
availability zone.

Tests tunables were adjusted manually through a number of ranges to initially produce the highest
possible throughput, and then parameters were adjusted to yield the lowest possible latency while
maintaining a throughput within 10% of the initially discovered peak.

### Quick Table

| SDK         | Avg. CPU | Docs/S  | Avg. Latency | Tuning                                  |
| ----------- | -------- | ------- | ------------ | --------------------------------------- |
| gocb        | 713%     | 26,418  | 2.10ms       | num-concurrent-ops=64                   |
| gocbcore    | 497%     | 168,356 | 2.55ms       | conn-pool-size=1,num-concurrent-ops=512 |
| gocbcore    | 688%     | 299,204 | 1.21ms       | conn-pool-size=8,num-concurrent-ops=512 |
| gocouch     | 783%     | 42,782  | 2.57ms       | num-concurrent-ops=128                  |
| gocouch     | 769%     | 39,555  | 5.98ms       | num-concurrent-ops=256                  |
| gocouchbulk | 660%     | 118,567 | 16.69ms      | num-concurrent-ops=256, batch-size=512  |

Note: Any unspecified tunning options here implies that the default values from the client are used.

### Details

#### gocb

```
$ ./cbgoclibench \
  ... \
  --doc-count=65536 \
  --duration=60s \
  --num-concurrent-ops=64 \
  gocb

Ran for 60029ms
Read 1560675 docs
Had 0 errors
Average docs/s of 26418.25
Average latency of 2.102503 milliseconds
```

#### gocbcore (default connection pool)

```
$ ./cbgoclibench \
  ... \
  --doc-count=65536 \
  --duration=60s \
  --conn-pool-size=1 \
  --max-queue-size=2048 \
  --num-concurrent-ops=512 \
  gocbcore

Ran for 60002ms
Read 10101712 docs
Had 0 errors
Average docs/s of 168356.25
Average latency of 2.556910 milliseconds
```

#### gocbcore (non-default connection pool)

```
$ ./cbgoclibench \
  ... \
  --doc-count=65536 \
  --duration=60s \
  --conn-pool-size=8 \
  --max-queue-size=16384 \
  --num-concurrent-ops=512 \
  gocbcore

Ran for 60001ms
Read 17952586 docs
Had 0 errors
Average docs/s of 299204.78
Average latency of 1.215389 milliseconds
```

#### gocouch

```
$ ./cbgoclibench \
  ... \
  --doc-count=65536 \
  --duration=60s \
  --num-concurrent-ops=128 \
  gocouch

Ran for 60024ms
Read 2567980 docs
Had 0 errors
Average docs/s of 42782.55
Average latency of 2.573395 milliseconds
```

Demonstration that increasing concurrent ops negatively
affects performance in go-couchbase:

```
$ ./cbgoclibench \
  ... \
  --doc-count=65536 \
  --duration=60s \
  --num-concurrent-ops=256 \
  gocouch

Ran for 60074ms
Read 2376232 docs
Had 0 errors
Average docs/s of 39555.08
Average latency of 5.989729 milliseconds
```

#### gocouchbulk

```
$ ./cbgoclibench \
  ... \
  --doc-count=65536 \
  --duration=60s \
  --num-concurrent-ops=4 \
  --batch-size=512 \
  gocouchbulk

Ran for 60016ms
Read 7115964 docs
Had 0 errors
Average docs/s of 118567.78
Average latency of 16.693686 milliseconds
```

Note, many variants of tuning this were attempted, the above
represents the best possible balance of performance without
significantly impacting latency.
