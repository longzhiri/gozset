// redis like sorted set
package zset

import (
	"container/heap"
	"fmt"
	"math"
)

type IntOrderedKey [2]int64 // 0:score 1:timeCounter

func newIntOrderedKey(score int32, timeCounter int64) IntOrderedKey {
	return IntOrderedKey{int64(score), timeCounter}
}

func (k IntOrderedKey) IsZero() bool {
	return k[0] == 0 && k[1] == 0
}

func (k IntOrderedKey) Score() int32 {
	return int32(k[0])
}

func (k IntOrderedKey) TimeCounter() int64 {
	return k[1]
}

type heapEntry struct {
	key   IntOrderedKey
	value int64
}

type heapMapInt struct {
	entries   []heapEntry
	less      func(l, r IntOrderedKey) bool
	key2Index map[IntOrderedKey]int
}

func newHeapMapInt(less func(l, r IntOrderedKey) bool) *heapMapInt {
	return &heapMapInt{
		less:      less,
		key2Index: make(map[IntOrderedKey]int),
	}
}

/*********************heap Interface implement***********************************/
func (h heapMapInt) Len() int {
	return len(h.entries)
}

func (h heapMapInt) Less(i, j int) bool {
	return h.less(h.entries[i].key, h.entries[j].key)
}

func (h heapMapInt) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
	h.key2Index[h.entries[i].key] = i
	h.key2Index[h.entries[j].key] = j
}

func (h *heapMapInt) Push(x interface{}) {
	entry := x.(heapEntry)
	h.key2Index[entry.key] = len(h.entries)
	h.entries = append(h.entries, entry)
}

func (h *heapMapInt) Pop() interface{} {
	old := h.entries
	n := len(old)
	entry := old[n-1]
	delete(h.key2Index, entry.key)
	h.entries = old[0 : n-1]
	return entry
}

/*********************************************/
func (h *heapMapInt) AddEntry(key IntOrderedKey, value int64) {
	heap.Push(h, heapEntry{key: key, value: value})
}

func (h *heapMapInt) DeleteEntry(key IntOrderedKey) bool {
	index, ok := h.key2Index[key]
	if !ok {
		return false
	}
	heap.Remove(h, index)
	return true
}

func (h *heapMapInt) PopEntry() (key IntOrderedKey, value int64) {
	if h.Len() == 0 {
		return IntOrderedKey{}, 0
	}
	entry := heap.Pop(h).(heapEntry)
	return entry.key, entry.value
}

func (h *heapMapInt) Clear() {
	h.entries = nil
	h.key2Index = make(map[IntOrderedKey]int)
}

type ZSetInt struct {
	key2OrderedKey map[int64]IntOrderedKey
	sl             *SkipListInt
	timeCounter    int64
	heapMap        *heapMapInt

	rankN          int
	leastSortedKey IntOrderedKey

	less func(l, r IntOrderedKey) bool
}

func NewZSetInt(scoreLessThan func(l, r int32) bool, rankN int) *ZSetInt {
	less := func(l, r IntOrderedKey) bool {
		if scoreLessThan(int32(l[0]), int32(r[0])) {
			return true
		} else if l[0] == r[0] {
			return l[1] < r[1]
		} else {
			return false
		}
	}
	return &ZSetInt{
		key2OrderedKey: make(map[int64]IntOrderedKey),
		sl:             NewSkipListInt(less),
		heapMap:        newHeapMapInt(less),
		rankN:          rankN,
		less:           less,
	}
}

func (z *ZSetInt) Exist(key int64) bool {
	_, ok := z.key2OrderedKey[key]
	return ok
}

func (z *ZSetInt) Add(key int64, score int32) bool {
	orderedKey, ok := z.key2OrderedKey[key]
	if ok {
		if score != orderedKey.Score() { // update
			z.doRemoveOldKey(key, orderedKey)
			z.doAddNewKey(key, score)
		}
	} else {
		z.doAddNewKey(key, score)
	}
	return true
}

func (z *ZSetInt) doRemoveOldKey(key int64, orderedKey IntOrderedKey) {
	if z.leastSortedKey.IsZero() || !z.less(z.leastSortedKey, orderedKey) { // 旧orderedKey在skipList
		z.sl.Delete(orderedKey)
		if z.heapMap.Len() > 0 {
			fillupSLKey, fillupKey := z.heapMap.PopEntry()
			z.sl.Set(fillupSLKey, fillupKey)
			if z.heapMap.Len() == 0 {
				z.leastSortedKey = IntOrderedKey{}
			} else {
				z.leastSortedKey = z.sl.SeekToLast().Key()
			}
		}
	} else { // 旧slKey在heapMap
		z.heapMap.DeleteEntry(orderedKey)
	}
	delete(z.key2OrderedKey, key)
}

func (z *ZSetInt) doAddNewKey(key int64, score int32) {
	z.timeCounter++
	orderedKey := newIntOrderedKey(score, z.timeCounter)

	if z.leastSortedKey.IsZero() || !z.less(z.leastSortedKey, orderedKey) { // 新slKey要放在skipList
		z.sl.Set(orderedKey, key)
		if z.rankN != 0 && z.sl.Len() > z.rankN { // overflow
			lastSortedNode := z.sl.SeekToLast()
			// delete last one from skiplist
			z.sl.Delete(lastSortedNode.Key())
			// reinsert into heapmap
			z.heapMap.AddEntry(lastSortedNode.Key(), lastSortedNode.Value())
			z.leastSortedKey = z.sl.SeekToLast().Key()
		}
	} else { // 新orderedKey要放在skipList
		z.heapMap.AddEntry(orderedKey, key)
	}
	z.key2OrderedKey[key] = orderedKey
}

func (z *ZSetInt) Update(key int64, score int32) bool {
	orderedKey, ok := z.key2OrderedKey[key]
	if !ok {
		return false
	}
	if score != orderedKey.Score() { // update
		z.doRemoveOldKey(key, orderedKey)
		z.doAddNewKey(key, score)
	}
	return true
}

func (z *ZSetInt) Remove(key int64) bool {
	orderedKey, ok := z.key2OrderedKey[key]
	if !ok {
		return false
	}
	z.doRemoveOldKey(key, orderedKey)
	return true
}

func (z *ZSetInt) Rank(key int64) uint32 {
	orderedKey, ok := z.key2OrderedKey[key]
	if !ok {
		return 0
	}
	if z.leastSortedKey.IsZero() || !z.less(z.leastSortedKey, orderedKey) { // slKey在skipList
		return z.sl.Rank(orderedKey)
	} else {
		return 0
	}
}

func (z *ZSetInt) Score(key int64) (int32, bool) {
	orderedKey, ok := z.key2OrderedKey[key]
	if !ok {
		return 0, false
	}
	return orderedKey.Score(), true
}

func (z *ZSetInt) RangeByRank(rankFrom uint32, rankTo uint32) [][2]int64 { // [rankFrom, rankTo]
	if rankTo > uint32(z.sl.Len()) {
		rankTo = uint32(z.sl.Len())
	}

	if rankTo < rankFrom {
		return nil
	}

	iter := z.sl.GetElemByRank(rankFrom)
	if iter == nil {
		return nil
	}
	keys := make([][2]int64, 0, int(rankTo-rankFrom+1))
	for i := rankFrom; i <= rankTo; i++ {
		keys = append(keys, [2]int64{iter.Value(), int64(iter.Key().Score())})
		if !iter.Next() {
			break
		}
	}
	return keys
}

func (z *ZSetInt) RangeByScore(scoreFrom int32, scoreTo int32) []int64 { // [scoreFrom, scoreTo]
	iter := z.sl.Range(newIntOrderedKey(scoreFrom, 0), newIntOrderedKey(scoreTo, math.MaxInt64))
	keys := make([]int64, 0, 8)
	rangeIter := iter.(*rangeIterator)
	for rangeIter.Next() {
		keys = append(keys, rangeIter.Value())
	}
	return keys
}

func (z *ZSetInt) Card() int { // 集合元素个数
	return len(z.key2OrderedKey)
}

func (z *ZSetInt) ForeachInOrder(fn func(key int64, score int32) bool) {
	iter := z.sl.Iterator()
	for iter.Next() {
		if !fn(iter.Value(), iter.Key().Score()) {
			break
		}
	}

	var poppedEntries []heapEntry
	for z.heapMap.Len() > 0 {
		entry := heap.Pop(z.heapMap).(heapEntry)
		poppedEntries = append(poppedEntries, entry)
		if !fn(entry.value, entry.key.Score()) {
			break
		}
	}

	// 重新加进去
	for _, entry := range poppedEntries {
		z.heapMap.AddEntry(entry.key, entry.value)
	}
}

func (z *ZSetInt) Clear() {
	z.key2OrderedKey = make(map[int64]IntOrderedKey)
	z.sl.Clear()
	z.timeCounter = 0

	z.leastSortedKey = IntOrderedKey{}
	z.heapMap.Clear()
}

func (z *ZSetInt) Marshal() (sortedSlice [][2]int64, heapSlice [][3]int64) {
	sortedSlice = make([][2]int64, 0, z.sl.Len())
	iter := z.sl.Iterator()
	for iter.Next() {
		sortedSlice = append(sortedSlice, [2]int64{iter.Value(), int64(iter.Key().Score())})
	}

	heapSlice = make([][3]int64, 0, z.heapMap.Len())
	for _, entry := range z.heapMap.entries {
		heapSlice = append(heapSlice, [3]int64{entry.value, int64(entry.key.Score()), entry.key.TimeCounter()})
	}

	return
}

func (z *ZSetInt) Unmarshal(sortedSlice [][2]int64, heapSlice [][3]int64) bool {
	orderedKeys := make([]IntOrderedKey, 0, len(sortedSlice))
	values := make([]int64, 0, len(sortedSlice))

	for _, elem := range sortedSlice {
		z.timeCounter++
		orderedKey := newIntOrderedKey(int32(elem[1]), z.timeCounter)
		z.key2OrderedKey[elem[0]] = orderedKey
		orderedKeys = append(orderedKeys, orderedKey)
		values = append(values, elem[0])
	}

	z.heapMap.entries = make([]heapEntry, 0, len(heapSlice))
	for _, elem := range heapSlice {
		orderedKey := newIntOrderedKey(int32(elem[1]), elem[2])
		z.heapMap.entries = append(z.heapMap.entries, heapEntry{key: orderedKey, value: elem[0]})
		z.heapMap.key2Index[orderedKey] = len(z.heapMap.entries) - 1
	}

	// 修正heapEntry的value的timeCounter
	for z.heapMap.Len() > 0 {
		orderedKey, key := z.heapMap.PopEntry()
		z.timeCounter++
		newOrderedKey := newIntOrderedKey(orderedKey.Score(), z.timeCounter)
		z.key2OrderedKey[key] = newOrderedKey
		orderedKeys = append(orderedKeys, newOrderedKey)
		values = append(values, key)
	}

	if z.rankN > 0 && len(orderedKeys) > z.rankN {
		z.sl.FillBySortedSlice(orderedKeys[:z.rankN], values[:z.rankN])
		orderedKeys = orderedKeys[z.rankN:]
		values = values[z.rankN:]
	} else {
		z.sl.FillBySortedSlice(orderedKeys, values)
		orderedKeys = nil
		values = nil
	}

	for i, orderedKey := range orderedKeys {
		z.heapMap.AddEntry(orderedKey, values[i])
	}

	if z.heapMap.Len() > 0 {
		z.leastSortedKey = z.sl.SeekToLast().Key()
	}

	return true
}

func (z *ZSetInt) UnmarshalMerge(sortedSlice1 [][2]int64, heapSlice1 [][3]int64, sortedSlice2 [][2]int64, heapSlice2 [][3]int64,
	filter [2]func(key int64) bool, modifier [2]func(key int64) int64) error {

	sortOne := func(sortedSlice [][2]int64, heapSlice [][3]int64, filter func(key int64) bool, modifier func(key int64) int64) (orderedKeys []IntOrderedKey, values []int64, err error) {
		orderedKeys = make([]IntOrderedKey, 0, len(sortedSlice))
		values = make([]int64, 0, len(sortedSlice))

		for _, elem := range sortedSlice {
			key := elem[0]
			if filter != nil && filter(key) {
				continue
			}
			if modifier != nil {
				key = modifier(key)
			}
			z.timeCounter++
			orderedKey := newIntOrderedKey(int32(elem[1]), z.timeCounter)
			if _, exist := z.key2OrderedKey[key]; exist {
				return nil, nil, fmt.Errorf("key=%v already exist", key)
			}
			z.key2OrderedKey[key] = orderedKey
			orderedKeys = append(orderedKeys, orderedKey)
			values = append(values, key)
		}

		z.heapMap.entries = make([]heapEntry, 0, len(heapSlice))
		z.heapMap.key2Index = make(map[IntOrderedKey]int)
		for _, elem := range heapSlice {
			orderedKey := newIntOrderedKey(int32(elem[1]), elem[2])
			z.heapMap.entries = append(z.heapMap.entries, heapEntry{key: orderedKey, value: elem[0]})
			z.heapMap.key2Index[orderedKey] = len(z.heapMap.entries) - 1
		}

		// 修正heapEntry的value的timeCounter
		for z.heapMap.Len() > 0 {
			orderedKey, key := z.heapMap.PopEntry()
			if filter != nil && filter(key) {
				continue
			}
			if modifier != nil {
				key = modifier(key)
			}
			z.timeCounter++
			newOrderedKey := newIntOrderedKey(orderedKey.Score(), z.timeCounter)
			if _, exist := z.key2OrderedKey[key]; exist {
				return nil, nil, fmt.Errorf("key=%v already exist", key)
			}
			z.key2OrderedKey[key] = newOrderedKey
			orderedKeys = append(orderedKeys, newOrderedKey)
			values = append(values, key)
		}
		return
	}

	orderedKeys1, values1, err := sortOne(sortedSlice1, heapSlice1, filter[0], modifier[0])
	if err != nil {
		return err
	}
	orderedKeys2, values2, err := sortOne(sortedSlice2, heapSlice2, filter[1], modifier[1])
	if err != nil {
		return err
	}

	// merge
	orderedKeys := make([]IntOrderedKey, len(orderedKeys1)+len(orderedKeys2))
	values := make([]int64, len(values1)+len(values2))
	var i1, i2 int
	for i := 0; i < len(values); i++ {
		if i1 < len(orderedKeys1) && (i2 >= len(orderedKeys2) || z.less(orderedKeys1[i1], orderedKeys2[i2])) {
			orderedKeys[i] = orderedKeys1[i1]
			values[i] = values1[i1]
			i1++
		} else {
			orderedKeys[i] = orderedKeys2[i2]
			values[i] = values2[i2]
			i2++
		}
	}

	// fill skiplist and heap
	if z.rankN > 0 && len(orderedKeys) > z.rankN {
		z.sl.FillBySortedSlice(orderedKeys[:z.rankN], values[:z.rankN])
		orderedKeys = orderedKeys[z.rankN:]
		values = values[z.rankN:]
	} else {
		z.sl.FillBySortedSlice(orderedKeys, values)
		orderedKeys = nil
		values = nil
	}

	for i, orderedKey := range orderedKeys {
		z.heapMap.AddEntry(orderedKey, values[i])
	}

	if z.heapMap.Len() > 0 {
		z.leastSortedKey = z.sl.SeekToLast().Key()
	}

	return nil
}
