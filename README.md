# gozset
An efficient redis-like zset golang implementation

## Usage
```go
// Create a ZSet with a less function and the rankNum of 50
var rankNum = 50
zs := NewZSetInt(func(l, r int32) bool {
	return l < r
}, rankNum)

// Add 10 key-value(i,10*i) pairs to the ZSet
for i := 0; i < 10; i++ {
	zs.Add(int64(i), int32(i)*10)
}

// Check the size of the ZSet 
if zs.Card() != 10 {
	log.Printf("The length of the ZSet should be 10")
}

// Check the Rank call
for i := 0; i < 10; i++ {
	if zs.Rank(int64(i)) != uint32(i+1) {
		log.Printf("Rank error")
	}
}

// Check the RangeByRank call
for i, ks := range zs.RangeByRank(1, 10000) {
	if ks[1] != int64(i*10) || ks[0] != int64(i) {
		log.Printf("RangeByRank error")
	}
}

// Check the RangeByScore call
for i, k := range zs.RangeByScore(0, 1000) {
	if k != int64(i) {
		log.Printf("RangByScore error")
	}
}

// Remove some keys
zs.Remove(5)
zs.Remove(6)
```
more examples in zset_int_test.go

##Benchmark
```Bash
BenchmarkZSetAdd-4                       1278654               862 ns/op             377 B/op          1 allocs/op
BenchmarkZSetAddNoHeapMap-4               926547              4383 ns/op            1207 B/op          4 allocs/op
BenchmarkZSetRemove-4                    1229588               936 ns/op              43 B/op          1 allocs/op
BenchmarkZSetRemoveNoHeapMap-4           1000000              3270 ns/op             256 B/op          1 allocs/op
BenchmarkZSetRank-4                      3538854               298 ns/op               0 B/op          0 allocs/op
BenchmarkZSetRankNoHeampMap-4            1405663               974 ns/op               0 B/op          0 allocs/op
BenchmarkZSetTopN-4                       132181              8300 ns/op            4912 B/op          2 allocs/op
BenchmarkZSetTopNNoHeapMap-4              133660              9044 ns/op            4912 B/op          2 allocs/op
BenchmarkZSetMarshal-4                     10000            220510 ns/op          229425 B/op          3 allocs/op
BenchmarkZSetMarshalNoHeapMap-4            10000            663226 ns/op          163888 B/op          2 allocs/op
```