package zset

import (
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestZSet(t *testing.T) {
	const maxElemNum = 100
	var rankNum = 50
	zs := NewZSetInt(func(l, r int32) bool {
		return l < r
	}, rankNum)
	for i := 0; i < maxElemNum; i++ {
		zs.Add(int64(i), int32(i)*10)
	}
	if zs.Card() != maxElemNum {
		t.Errorf("after add 100, zset length should be 100")
	}

	for i := 0; i < rankNum; i++ {
		if zs.Rank(int64(i)) != uint32(i+1) {
			t.Errorf("rank error")
		}
	}

	for i, ks := range zs.RangeByRank(1, 10000) {
		if ks[1] != int64(i*10) || ks[0] != int64(i) {
			t.Errorf("rangebyrank error")
		}
	}

	for i, k := range zs.RangeByScore(0, 1000) {
		if k != int64(i) {
			t.Errorf("rangbyscore error")
		}
	}

	var removedNum int
	for i := 0; i < maxElemNum; i++ {
		if i%3 == 0 {
			ok := zs.Remove(int64(i))
			if !ok {
				t.Errorf("remove failed")
			}
			removedNum++
		}
	}
	if zs.Card() != maxElemNum-removedNum {
		t.Errorf("after remove 50, zset length should be 50")
	}

	sortedSlice, heapSlice := zs.Marshal()
	var lastKey int64 = -1
	for _, elem := range sortedSlice {
		if elem[0]%3 == 0 {
			t.Errorf("marshal error")
		}
		if elem[0]*10 != elem[1] {
			t.Errorf("marshal heapSlice error")
		}
		if lastKey >= elem[0] {
			t.Errorf("marshal not in order")
		}
		lastKey = elem[0]
	}
	for i, elem := range heapSlice {
		li := (i+1)*2 - 1
		ri := (i + 1) * 2
		if li < len(heapSlice) && elem[1] >= heapSlice[li][1] {
			t.Errorf("marshal invalid heapSlice")
		}
		if ri < len(heapSlice) && elem[1] >= heapSlice[ri][1] {
			t.Errorf("marshal invalid heapSlice")
		}
	}

	zs.Clear()

	zs.Unmarshal(sortedSlice, heapSlice)

	var lastScore int32 = -1
	zs.ForeachInOrder(func(key int64, score int32) bool {
		if int32(key*10) != score {
			t.Errorf("key score not match")
		}
		if lastScore >= score {
			t.Errorf("score not in order")
		}
		lastScore = score
		return true
	})

	zs.Clear()

	zs.UnmarshalMerge(sortedSlice, heapSlice, sortedSlice, heapSlice, [2]func(key int64) bool{}, [2]func(key int64) int64{})
	lastScore = -1
	zs.ForeachInOrder(func(key int64, score int32) bool {
		if int32(key*10) != score {
			t.Errorf("key score not match")
		}
		if lastScore == -1 {
			lastScore = score
		} else {
			if lastScore != score {
				t.Errorf("score not in order")
			}
			lastScore = -1
		}
		return true
	})
}

func TestZSetRank(t *testing.T) {

}

func shuffleArray(array []int) {
	for len(array) != 0 {
		pos := rand.Intn(len(array))
		array[0], array[pos] = array[pos], array[0]
		array = array[1:]
	}
}

func TestZSet2(t *testing.T) {
	rand.Seed(time.Now().Unix())
	const length = 1000000
	var rankNum int = int(rand.Int31n(length))
	zs := NewZSetInt(func(l, r int32) bool {
		return l < r
	}, rankNum)

	array := make([]int, length)
	for i := 0; i < length; i++ {
		array[i] = i
	}
	shuffleArray(array)
	for _, v := range array {
		zs.Add(int64(v), int32(v))
	}
	for _, v := range array {
		if v+1 > rankNum {
			continue
		}
		if zs.Rank(int64(v)) != uint32(v+1) {
			t.Fatalf("rank perform wrong")
		}
	}

	rankFrom := uint32(rand.Intn(len(array))) + 1
	for i, ks := range zs.RangeByRank(rankFrom, uint32(len(array))) {
		if uint32(ks[0]+1) != uint32(i)+rankFrom {
			t.Fatalf("range by rank perform wrong")
		}
	}

	sortedSlice, heapSlice := zs.Marshal()

	var lastKey int64 = -1
	for _, elem := range sortedSlice {
		if lastKey >= elem[0] {
			t.Errorf("marshal not in order")
		}
		lastKey = elem[0]
	}
	for i, elem := range heapSlice {
		li := (i+1)*2 - 1
		ri := (i + 1) * 2
		if li < len(heapSlice) && elem[1] >= heapSlice[li][1] {
			t.Errorf("marshal invalid heapSlice")
		}
		if ri < len(heapSlice) && elem[1] >= heapSlice[ri][1] {
			t.Errorf("marshal invalid heapSlice")
		}
	}

	zs.Clear()
	zs.Unmarshal(sortedSlice, heapSlice)

	var lastScore int32 = -1
	zs.ForeachInOrder(func(key int64, score int32) bool {
		if key != int64(score) {
			t.Errorf("key score not match")
		}
		if lastScore >= score {
			t.Errorf("score not in order")
		}
		lastScore = score
		return true
	})

	for _, v := range array {
		if v+1 > rankNum {
			continue
		}
		if zs.Rank(int64(v)) != uint32(v+1) {
			t.Fatalf("rank perform wrong")
		}
	}

	for _, v := range array {
		zs.Update(int64(v), int32(-v))
	}

	zs.ForeachInOrder(func(key int64, score int32) bool {
		if key != int64(-score) {
			t.Fatalf("foreach perform wrong")
			//return false
		}
		return true
	})

	for _, v := range array {
		zs.Remove(int64(v))
	}

	if zs.RangeByRank(100, 300) != nil {
		t.Fatalf("range by rank perform wrong")
	}

	if zs.Rank(1) != 0 {
		t.Fatalf("rank perform wrong")
	}

	if zs.Update(1, 99) {
		t.Fatalf("update perform wrong")
	}

	if zs.Remove(1) {
		t.Fatalf("remove perform wrong")
	}
}

func TestZSetSameScore(t *testing.T) {
	rand.Seed(time.Now().Unix())
	const length = 1000000
	type userInfo struct {
		uid    int64
		points int32
		tc     int64
	}
	userList := make([]userInfo, 0, length)
	arrayIndex := make([]int, length)
	pts := 10
	for i := 0; i < length; i++ {
		arrayIndex[i] = i
		if rand.Intn(2) == 0 {
			pts += 10
		}
		userList = append(userList, userInfo{
			uid:    int64(i),
			points: int32(pts),
		})
	}

	var rankNum int = int(rand.Int31n(length))
	zs := NewZSetInt(func(l, r int32) bool {
		return l < r
	}, rankNum)

	shuffleArray(arrayIndex)
	var timeCounter int64
	for _, v := range arrayIndex {
		zs.Add(userList[v].uid, userList[v].points)
		timeCounter++
		userList[v].tc = timeCounter
	}

	sort.Slice(userList, func(i, j int) bool {
		if userList[i].points < userList[j].points {
			return true
		} else if userList[i].points == userList[j].points && userList[i].tc < userList[j].tc {
			return true
		} else {
			return false
		}
	})

	for i := 0; i < rankNum; i++ {
		if zs.Rank(userList[i].uid) != uint32(i+1) {
			t.Fatalf("rank error")
		}
	}

	var rank uint32
	zs.ForeachInOrder(func(key int64, score int32) bool {
		if key != userList[rank].uid {
			t.Fatalf("zset not in order")
		}
		if score != userList[rank].points {
			t.Fatalf("score is not right")
		}
		if key%3 == 0 {
			userList[rank].uid = -1
		}
		rank++
		return true
	})

	for i := 0; i < length; i++ {
		if i%3 == 0 {
			if !zs.Remove(int64(i)) {
				t.Fatalf("remove failed")
			}
		}
	}

	rank = 0
	zs.ForeachInOrder(func(key int64, score int32) bool {
		for userList[rank].uid == -1 {
			rank++
		}
		if userList[rank].uid != key {
			t.Fatalf("zset not in order")
		}
		rank++
		return true
	})

	sortedSlice, heapSlice := zs.Marshal()
	for i, elem := range heapSlice {
		li := (i+1)*2 - 1
		ri := (i + 1) * 2
		if li < len(heapSlice) && (elem[1] > heapSlice[li][1] || (elem[1] == heapSlice[li][1] && elem[2] >= heapSlice[li][2])) {
			t.Fatalf("marshal invalid heapSlice")
		}
		if ri < len(heapSlice) && (elem[1] > heapSlice[ri][1] || (elem[1] == heapSlice[ri][1] && elem[2] >= heapSlice[ri][2])) {
			t.Fatalf("marshal invalid heapSlice")
		}
	}
	zs.Clear()
	zs.Unmarshal(sortedSlice, heapSlice)
	sortedSlice2, heapSlice2 := zs.Marshal()
	for i, elem := range heapSlice2 {
		if elem[0] != heapSlice[i][0] || elem[1] != heapSlice[i][1] {
			t.Fatalf("heapSlice mismatched")
		}
	}
	for i, elem := range sortedSlice2 {
		if elem != sortedSlice[i] {
			t.Fatalf("sortedSlice mismatched")
		}
	}

	rank = 0
	zs.ForeachInOrder(func(key int64, score int32) bool {
		for userList[rank].uid == -1 {
			rank++
		}
		if userList[rank].uid != key {
			t.Fatalf("zset not in order")
		}
		rank++
		return true
	})
}

func bZSetAdd(b *testing.B, rankNum int) {
	rand.Seed(time.Now().Unix())

	zs := NewZSetInt(func(l, r int32) bool {
		return l < r
	}, rankNum)

	array := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		array[i] = i
	}
	shuffleArray(array)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		zs.Add(int64(i), int32(array[i]))
	}
}

func BenchmarkZSetAdd(b *testing.B) {
	bZSetAdd(b, 1500)
}

func BenchmarkZSetAddNoHeapMap(b *testing.B) {
	bZSetAdd(b, 0)
}

func bZSetRemove(b *testing.B, rankNum int) {
	rand.Seed(time.Now().Unix())

	zs := NewZSetInt(func(l, r int32) bool {
		return l < r
	}, rankNum)

	array := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		array[i] = i
	}
	shuffleArray(array)
	for i := 0; i < b.N; i++ {
		zs.Add(int64(array[i]), int32(array[i]))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		zs.Remove(int64(array[i]))
	}
}

func BenchmarkZSetRemove(b *testing.B) {
	bZSetRemove(b, 1500)
}

func BenchmarkZSetRemoveNoHeapMap(b *testing.B) {
	bZSetRemove(b, 0)
}

func bZSetRank(b *testing.B, rankNum int) {
	rand.Seed(time.Now().Unix())

	zs := NewZSetInt(func(l, r int32) bool {
		return l < r
	}, rankNum)

	array := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		array[i] = i
	}
	shuffleArray(array)
	for i := 0; i < b.N; i++ {
		zs.Add(int64(array[i]), int32(array[i]))
	}
	b.ResetTimer()
	if rankNum == 0 {
		rankNum = b.N
	}
	for i := 0; i < b.N; i++ {
		k := i % rankNum
		if zs.Rank(int64(k)) != uint32(k+1) {
			b.Fatalf("rank perform wrong")
		}
	}
}

func BenchmarkZSetRank(b *testing.B) {
	bZSetRank(b, 1500)
}

func BenchmarkZSetRankNoHeampMap(b *testing.B) {
	bZSetRank(b, 0)
}

func bZSetTopN(b *testing.B, rankNum int) {
	rand.Seed(time.Now().Unix())

	zs := NewZSetInt(func(l, r int32) bool {
		return l < r
	}, rankNum)

	array := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		array[i] = i
	}
	shuffleArray(array)
	for i := 0; i < b.N; i++ {
		zs.Add(int64(array[i]), int32(array[i]))
	}
	b.ResetTimer()
	const topN = 300
	for i := 0; i < b.N; i++ {
		res := zs.RangeByRank(1, topN)
		for j := 0; j < len(res); j++ {
			if res[j][0] != int64(j) || res[j][1] != int64(j) {
				b.Fatalf("topn perform wrong")
			}
		}
	}
}
func BenchmarkZSetTopN(b *testing.B) {
	bZSetTopN(b, 1500)
}

func BenchmarkZSetTopNNoHeapMap(b *testing.B) {
	bZSetTopN(b, 0)
}

func bZSetMarshal(b *testing.B, rankNum int) {
	rand.Seed(time.Now().Unix())

	zs := NewZSetInt(func(l, r int32) bool {
		return l < r
	}, rankNum)

	array := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		array[i] = i
	}
	shuffleArray(array)
	for i := 0; i < b.N; i++ {
		zs.Add(int64(array[i]), int32(array[i]))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sortedSlice, heapSlice := zs.Marshal()
		if len(sortedSlice)+len(heapSlice) != zs.Card() {
			b.Fatalf("marshal perform wrong")
		}
	}
}
func BenchmarkZSetMarshal(b *testing.B) {
	bZSetMarshal(b, 1500)
}
func BenchmarkZSetMarshalNoHeapMap(b *testing.B) {
	bZSetMarshal(b, 0)
}
