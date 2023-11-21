package main

import "testing"

func TestDeserializeSimpleString(t *testing.T) {
	input := "+OK\r\n"
	want := "OK"
	if result, _, _ := deserializeSimpleString(input); result != want {
		t.Errorf("Got '%s' but expected '%s'.", result, want)
	}
}
