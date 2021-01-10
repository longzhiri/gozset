// Copyright 2012 Google Inc. All rights reserved.
// Author: Ric Szopa (Ryszard) <ryszard.szopa@gmail.com>

// Package skiplist implements skip list based maps and sets.
//
// Skip lists are a data structure that can be used in place of
// balanced trees. Skip lists use probabilistic balancing rather than
// strictly enforced balancing and as a result the algorithms for
// insertion and deletion in skip lists are much simpler and
// significantly faster than equivalent algorithms for balanced trees.
//
// Skip lists were first described in Pugh, William (June 1990). "Skip
// lists: a probabilistic alternative to balanced
// trees". Communications of the ACM 33 (6): 668–676
package zset

import (
	"math/rand"
)

// TODO(ryszard):
//   - A separately seeded source of randomness

// p is the fraction of nodes with level i pointers that also have
// level i+1 pointers. p equal to 1/4 is a good value from the point
// of view of speed and space requirements. If variability of running
// times is a concern, 1/2 is a better value for p.
const p = 0.25

const DefaultMaxLevel = 32

// A node is a container for key-value pairs that are stored in a skip
// list.
type level struct {
	forward *node
	span    uint32
}

type node struct {
	levels   []level
	backward *node
	key      IntOrderedKey
	value    int64
}

// next returns the next node in the skip list containing n.
func (n *node) next() *node {
	if len(n.levels) == 0 {
		return nil
	}
	return n.levels[0].forward
}

// previous returns the previous node in the skip list containing n.
func (n *node) previous() *node {
	return n.backward
}

// hasNext returns true if n has a next node.
func (n *node) hasNext() bool {
	return n.next() != nil
}

// hasPrevious returns true if n has a previous node.
func (n *node) hasPrevious() bool {
	return n.previous() != nil
}

// A SkipListInt is a map-like data structure that maintains an ordered
// collection of key-value pairs. Insertion, lookup, and deletion are
// all O(log n) operations. A SkipListInt can efficiently store up to
// 2^MaxLevel items.
//
// To iterate over a skip list (where s is a
// *SkipListInt):
//
//	for i := s.Iterator(); i.Next(); {
//		// do something with i.Key() and i.Value()
//	}
type SkipListInt struct {
	lessThan func(l, r IntOrderedKey) bool
	header   *node
	footer   *node
	length   int
	// MaxLevel determines how many items the SkipList can store
	// efficiently (2^MaxLevel).
	//
	// It is safe to increase MaxLevel to accomodate more
	// elements. If you decrease MaxLevel and the skip list
	// already contains nodes on higer levels, the effective
	// MaxLevel will be the greater of the new MaxLevel and the
	// level of the highest node.
	//
	// A SkipListInt with MaxLevel equal to 0 is equivalent to a
	// standard linked list and will not have any of the nice
	// properties of skip lists (probably not what you want).
	MaxLevel int
}

// Len returns the length of s.
func (s *SkipListInt) Len() int {
	return s.length
}

func (s *SkipListInt) Clear() {
	s.header = &node{
		levels: []level{level{}},
	}
	s.footer = nil
	s.length = 0
}

// Iterator is an interface that you can use to iterate through the
// skip list (in its entirety or fragments). For an use example, see
// the documentation of SkipListInt.
//
// Key and Value return the key and the value of the current node.
type Iterator interface {
	// Next returns true if the iterator contains subsequent elements
	// and advances its state to the next element if that is possible.
	Next() (ok bool)
	// Previous returns true if the iterator contains previous elements
	// and rewinds its state to the previous element if that is possible.
	Previous() (ok bool)
	// Key returns the current key.
	Key() IntOrderedKey
	// Value returns the current value.
	Value() int64
	// Seek reduces iterative seek costs for searching forward into the Skip List
	// by remarking the range of keys over which it has scanned before.  If the
	// requested key occurs prior to the point, the Skip List will start searching
	// as a safeguard.  It returns true if the key is within the known range of
	// the list.
	Seek(key IntOrderedKey) (ok bool)
	// Close this iterator to reap resources associated with it.  While not
	// strictly required, it will provide extra hints for the garbage collector.
	Close()
}

type iter struct {
	current *node
	key     IntOrderedKey
	list    *SkipListInt
	value   int64
}

func (i iter) Key() IntOrderedKey {
	return i.key
}

func (i iter) Value() int64 {
	return i.value
}

func (i *iter) Next() bool {
	if !i.current.hasNext() {
		return false
	}

	i.current = i.current.next()
	i.key = i.current.key
	i.value = i.current.value

	return true
}

func (i *iter) Previous() bool {
	if !i.current.hasPrevious() {
		return false
	}

	i.current = i.current.previous()
	i.key = i.current.key
	i.value = i.current.value

	return true
}

func (i *iter) Seek(key IntOrderedKey) (ok bool) {
	current := i.current
	list := i.list

	// If the existing iterator outside of the known key range, we should set the
	// position back to the beginning of the list.
	if current == nil {
		current = list.header
	}

	// If the target key occurs before the current key, we cannot take advantage
	// of the heretofore spent traversal cost to find it; resetting back to the
	// beginning is the safest choice.
	if !current.key.IsZero() && list.lessThan(key, current.key) {
		current = list.header
	}

	// We should back up to the so that we can seek to our present value if that
	// is requested for whatever reason.
	if current.backward == nil {
		current = list.header
	} else {
		current = current.backward
	}

	current = list.getLowerBound(current, key)

	if current == nil {
		return
	}

	i.current = current
	i.key = current.key
	i.value = current.value

	return true
}

func (i *iter) Close() {
	i.key = IntOrderedKey{}
	i.value = 0
	i.current = nil
	i.list = nil
}

type rangeIterator struct {
	iter
	upperLimit IntOrderedKey
	lowerLimit IntOrderedKey
}

func (i *rangeIterator) Next() bool {
	if !i.current.hasNext() {
		return false
	}

	next := i.current.next()

	if !i.list.lessThan(next.key, i.upperLimit) {
		return false
	}

	i.current = i.current.next()
	i.key = i.current.key
	i.value = i.current.value
	return true
}

func (i *rangeIterator) Previous() bool {
	if !i.current.hasPrevious() {
		return false
	}

	previous := i.current.previous()

	if i.list.lessThan(previous.key, i.lowerLimit) {
		return false
	}

	i.current = i.current.previous()
	i.key = i.current.key
	i.value = i.current.value
	return true
}

func (i *rangeIterator) Seek(key IntOrderedKey) (ok bool) {
	if i.list.lessThan(key, i.lowerLimit) {
		return
	} else if !i.list.lessThan(key, i.upperLimit) {
		return
	}

	return i.iter.Seek(key)
}

func (i *rangeIterator) Close() {
	i.iter.Close()
	i.upperLimit = IntOrderedKey{}
	i.lowerLimit = IntOrderedKey{}
}

// Iterator returns an Iterator that will go through all elements s.
func (s *SkipListInt) Iterator() Iterator {
	return &iter{
		current: s.header,
		list:    s,
	}
}

// Seek returns a bidirectional iterator starting with the first element whose
// key is greater or equal to key; otherwise, a nil iterator is returned.
func (s *SkipListInt) Seek(key IntOrderedKey) Iterator {
	current := s.getLowerBound(s.header, key)
	if current == nil {
		return nil
	}

	return &iter{
		current: current,
		key:     current.key,
		list:    s,
		value:   current.value,
	}
}

// SeekToFirst returns a bidirectional iterator starting from the first element
// in the list if the list is populated; otherwise, a nil iterator is returned.
func (s *SkipListInt) SeekToFirst() Iterator {
	if s.length == 0 {
		return nil
	}

	current := s.header.next()

	return &iter{
		current: current,
		key:     current.key,
		list:    s,
		value:   current.value,
	}
}

// SeekToLast returns a bidirectional iterator starting from the last element
// in the list if the list is populated; otherwise, a nil iterator is returned.
func (s *SkipListInt) SeekToLast() Iterator {
	current := s.footer
	if current == nil {
		return nil
	}

	return &iter{
		current: current,
		key:     current.key,
		list:    s,
		value:   current.value,
	}
}

// Range returns an iterator that will go through all the
// elements of the skip list that are greater or equal than from, but
// less than to.
func (s *SkipListInt) Range(from, to IntOrderedKey) Iterator {
	start := s.getLowerBound(s.header, from)
	return &rangeIterator{
		iter: iter{
			current: &node{
				levels:   []level{level{start, 0}},
				backward: start,
			},
			list: s,
		},
		upperLimit: to,
		lowerLimit: from,
	}
}

func (s *SkipListInt) level() int {
	return len(s.header.levels) - 1
}

func maxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func (s *SkipListInt) effectiveMaxLevel() int {
	return maxInt(s.level(), s.MaxLevel)
}

// Returns a new random level.
func (s SkipListInt) randomLevel() (n int) {
	for n = 0; n < s.effectiveMaxLevel() && rand.Float64() < p; n++ {
	}
	return
}

// Get returns the value associated with key from s (nil if the key is
// not present in s). The second return value is true when the key is
// present.
func (s *SkipListInt) Get(key IntOrderedKey) (value int64, ok bool) {
	candidate := s.getLowerBound(s.header, key)

	if candidate == nil || candidate.key != key {
		return 0, false
	}

	return candidate.value, true
}

// GetGreaterOrEqual finds the node whose key is greater than or equal
// to min. It returns its value, its actual key, and whether such a
// node is present in the skip list.
func (s *SkipListInt) GetGreaterOrEqual(min IntOrderedKey) (actualKey IntOrderedKey, value int64, ok bool) {
	candidate := s.getLowerBound(s.header, min)

	if candidate != nil {
		return candidate.key, candidate.value, true
	}
	return IntOrderedKey{}, 0, false
}

func (s *SkipListInt) Rank(key IntOrderedKey) uint32 {
	current := s.header
	var rank uint32
	for i := s.level(); i >= 0; i-- {
		for current.levels[i].forward != nil && s.lessThan(current.levels[i].forward.key, key) {
			rank += current.levels[i].span
			current = current.levels[i].forward
		}
		if current.levels[i].forward != nil && current.levels[i].forward.key == key {
			return rank + current.levels[i].span
		}
	}
	return 0
}

func (s *SkipListInt) GetElemByRank(rank uint32) Iterator {
	current := s.header
	var traversed uint32
	for i := s.level(); i >= 0; i-- {
		for current.levels[i].forward != nil && (traversed+current.levels[i].span < rank) {
			traversed += current.levels[i].span
			current = current.levels[i].forward
		}
		if current.levels[i].forward != nil && traversed+current.levels[i].span == rank {
			return &iter{
				current: current.levels[i].forward,
				key:     current.levels[i].forward.key,
				list:    s,
				value:   current.levels[i].forward.value,
			}
		}
	}
	return nil
}

func (s *SkipListInt) getLowerBound(current *node, key IntOrderedKey) *node {
	depth := len(current.levels) - 1

	for i := depth; i >= 0; i-- {
		for current.levels[i].forward != nil && s.lessThan(current.levels[i].forward.key, key) {
			current = current.levels[i].forward
		}
		if current.levels[i].forward != nil && current.levels[i].forward.key == key {
			return current.levels[i].forward
		}
	}
	return current.next()
}

func (s *SkipListInt) searchForInsert(key IntOrderedKey, update []*node, rank []uint32) *node {
	current := s.header
	for i := s.level(); i >= 0; i-- {
		if i == s.level() {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for current.levels[i].forward != nil && s.lessThan(current.levels[i].forward.key, key) {
			rank[i] += current.levels[i].span
			current = current.levels[i].forward
		}
		if current.levels[i].forward != nil && current.levels[i].forward.key == key {
			return current.levels[i].forward
		}
		update[i] = current
	}
	return current.next()
}

// Sets set the value associated with key in s.
func (s *SkipListInt) Set(key IntOrderedKey, value int64) {
	if key.IsZero() {
		panic("goskiplist: zero value keys are not supported")
	}
	// s.level starts from 0, so we need to allocate one.
	update := make([]*node, s.level()+1, s.effectiveMaxLevel()+1)
	rank := make([]uint32, s.level()+1, s.effectiveMaxLevel()+1)
	candidate := s.searchForInsert(key, update, rank)

	if candidate != nil && candidate.key == key {
		candidate.value = value
		return
	}

	newLevel := s.randomLevel()

	if currentLevel := s.level(); newLevel > currentLevel {
		// there are no pointers for the higher levels in
		// update. Header should be there. Also add higher
		// level links to the header.
		for i := currentLevel + 1; i <= newLevel; i++ {
			s.header.levels = append(s.header.levels, level{})
			rank = append(rank, 0)
			update = append(update, s.header)
			update[i].levels[i].span = uint32(s.length)
		}
	}

	newNode := &node{
		levels: make([]level, newLevel+1, s.effectiveMaxLevel()+1),
		key:    key,
		value:  value,
	}

	if previous := update[0]; !previous.key.IsZero() {
		newNode.backward = previous
	}

	for i := 0; i <= newLevel; i++ {
		newNode.levels[i].forward = update[i].levels[i].forward
		update[i].levels[i].forward = newNode

		newNode.levels[i].span = update[i].levels[i].span - (rank[0] - rank[i])
		update[i].levels[i].span = (rank[0] - rank[i]) + 1
	}

	for i := newLevel + 1; i <= s.level(); i++ {
		update[i].levels[i].span++
	}

	s.length++

	if newNode.levels[0].forward != nil {
		if newNode.levels[0].forward.backward != newNode {
			newNode.levels[0].forward.backward = newNode
		}
	}

	if s.footer == nil || s.lessThan(s.footer.key, key) {
		s.footer = newNode
	}
}

func (s *SkipListInt) FillBySortedSlice(keys []IntOrderedKey, values []int64) bool {
	if s.Len() != 0 {
		panic("goskiplist: can only fill empty skiplist")
	}

	update := make([]*node, s.level()+1, s.effectiveMaxLevel()+1)
	update[0] = s.header

	for pos, key := range keys {
		newLevel := s.randomLevel()

		if currentLevel := s.level(); newLevel > currentLevel {
			// there are no pointers for the higher levels in
			// update. Header should be there. Also add higher
			// level links to the header.
			for i := currentLevel + 1; i <= newLevel; i++ {
				s.header.levels = append(s.header.levels, level{})
				update = append(update, s.header)
				update[i].levels[i].span = uint32(pos)
			}
		}

		newNode := &node{
			levels: make([]level, newLevel+1, s.effectiveMaxLevel()+1),
			key:    key,
			value:  values[pos],
		}

		if update[0] != s.header {
			newNode.backward = update[0]
			if !s.lessThan(update[0].key, newNode.key) {
				panic("goskiplist: fill by unsorted slice")
			}
		}

		for i := 0; i <= newLevel; i++ {
			update[i].levels[i].forward = newNode
			update[i].levels[i].span++
			update[i] = newNode
		}

		for i := newLevel + 1; i <= s.level(); i++ {
			update[i].levels[i].span++
		}

		s.footer = newNode
		s.length++
	}
	return true
}

func (s *SkipListInt) searchForDelete(current *node, key IntOrderedKey, update []*node) *node {
	depth := len(current.levels) - 1

	for i := depth; i >= 0; i-- {
		for current.levels[i].forward != nil && s.lessThan(current.levels[i].forward.key, key) {
			current = current.levels[i].forward
		}
		update[i] = current
	}
	return current.next()
}

// Delete removes the node with the given key.
//
// It returns the old value and whether the node was present.
func (s *SkipListInt) Delete(key IntOrderedKey) (value int64, ok bool) {
	if key.IsZero() {
		panic("goskiplist: 0 keys are not supported")
	}
	update := make([]*node, s.level()+1, s.effectiveMaxLevel())
	candidate := s.searchForDelete(s.header, key, update)

	if candidate == nil || candidate.key != key {
		return 0, false
	}

	previous := candidate.backward
	if s.footer == candidate {
		s.footer = previous
	}

	next := candidate.next()
	if next != nil {
		next.backward = previous
	}

	for i := 0; i <= s.level(); i++ {
		if update[i].levels[i].forward == candidate {
			update[i].levels[i].span += candidate.levels[i].span - 1
			update[i].levels[i].forward = candidate.levels[i].forward
		} else {
			update[i].levels[i].span -= 1
		}
	}

	for s.level() > 0 && s.header.levels[s.level()].forward == nil {
		s.header.levels = s.header.levels[:s.level()]
	}
	s.length--

	return candidate.value, true
}

// NewCustomMap returns a new SkipListInt that will use lessThan as the
// comparison function. lessThan should define a linear order on keys
// you intend to use with the SkipListInt.
func NewSkipListInt(lessThan func(l, r IntOrderedKey) bool) *SkipListInt {
	return &SkipListInt{
		header: &node{
			levels: []level{level{}},
		},
		MaxLevel: DefaultMaxLevel,
		lessThan: lessThan,
	}
}
