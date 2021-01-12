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