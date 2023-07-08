package keyvaluestore

import (
	"errors"
	"testing"
)

func TestPut(t *testing.T) {
	t.Parallel()
	const key = "create-key"
	const value = "create-value"

	var val interface{}
	var contains bool

	defer delete(store.m, key)

	_, contains = store.m[key]
	if contains {
		t.Error("key/value already exists")
	}

	err := Put(key, value)
	if err != nil {
		t.Error(err)
	}

	val, contains = store.m[key]
	if !contains {
		t.Error("create failed")
	}

	if val != value {
		t.Error("val/value mismatch")
	}

}

func TestGet(t *testing.T) {
	t.Parallel()
	const key = "create-key"
	const value = "create-value"

	var val interface{}
	var err error

	defer delete(store.m, key)

	val, err = Get(key)
	if err == nil {
		t.Error("expected an error")
	}
	if !errors.Is(err, ErrorNoSuchKey) {
		t.Error("unexpected error:", err)
	}

	store.m[key] = value

	val, err = Get(key)
	if err != nil {
		t.Error("unexpected error:", err)
	}
	if val != value {
		t.Error("val/value mismatch")
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	const key = "create-key"
	const value = "create-value"

	var contains bool
	defer delete(store.m, key)

	store.m[key] = value

	_, contains = store.m[key]
	if !contains {
		t.Error("key/value doesn't exist")
	}

	Delete(key)

	_, contains = store.m[key]
	if contains {
		t.Error("Delete failed")
	}
}
