package main

import "testing"

func TestEventDeduplicatorRejectsDuplicateID(t *testing.T) {
	deduplicator := NewEventDeduplicator(10)

	if !deduplicator.Remember("evt-001") {
		t.Fatal("expected first event id to be remembered")
	}

	if deduplicator.Remember("evt-001") {
		t.Fatal("expected duplicate event id to be rejected")
	}
}

func TestEventDeduplicatorEvictsOldestID(t *testing.T) {
	deduplicator := NewEventDeduplicator(2)

	deduplicator.Remember("evt-001")
	deduplicator.Remember("evt-002")
	deduplicator.Remember("evt-003")

	if deduplicator.Len() != 2 {
		t.Fatalf("expected deduplicator length 2, got %d", deduplicator.Len())
	}

	if !deduplicator.Remember("evt-001") {
		t.Fatal("expected oldest event id to have been evicted")
	}
}
