package main

import "sync"

type node struct {
	value string
	next  *node
	prev  *node
}

type linkedList struct {
	length uint
	head   *node
	tail   *node
}

type record struct {
	value           interface{} // string or LinkedList
	expiryTimestamp int64
}

type dictionary struct {
	mu   sync.Mutex
	dict map[string]record
}

func newStore() *dictionary {
	store := &dictionary{
		dict: map[string]record{},
	}
	return store
}
