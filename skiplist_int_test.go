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
	"fmt"
	"testing"
)

func (s *SkipListInt) printRepr() {

	fmt.Printf("header:\n")
	for i, level := range s.header.levels {
		if level.forward != nil {
			fmt.Printf("\t%d: -> %v\n", i, level.forward.key)
		} else {
			fmt.Printf("\t%d: -> END\n", i)
		}
	}

	for node := s.header.next(); node != nil; node = node.next() {
		fmt.Printf("%v: %v (level %d)\n", node.key, node.value, len(node.levels))
		for i, level := range node.levels {
			if level.forward != nil {
				fmt.Printf("\t%d: -> %v\n", i, level.forward.key)
			} else {
				fmt.Printf("\t%d: -> END\n", i)
			}
		}
	}
	fmt.Println()
}

func TestEmptyNodeNext(t *testing.T) {
	n := new(node)
	if next := n.next(); next != nil {
		t.Errorf("Next() should be nil for an empty node.")
	}

	if n.hasNext() {
		t.Errorf("hasNext() should be false for an empty node.")
	}
}

func TestEmptyNodePrev(t *testing.T) {
	n := new(node)
	if previous := n.previous(); previous != nil {
		t.Errorf("Previous() should be nil for an empty node.")
	}

	if n.hasPrevious() {
		t.Errorf("hasPrevious() should be false for an empty node.")
	}
}
