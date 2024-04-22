package main

import (
	"testing"
)

func TestParseLine(t *testing.T) {
	k, v, err := parseLine([]byte("hi;-42.5"))
	if err != nil {
		t.Error(err)
	}

	if k != "hi" || v != -42.5 {
		t.Error(k, v)
	}
}

func BenchmarkParseLine(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseLine([]byte("hithere;42.1"))
	}
}
