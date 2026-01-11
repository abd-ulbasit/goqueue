package broker

import (
	"reflect"
	"testing"
)

func TestAssignmentCodec_RoundTrip(t *testing.T) {
	// =============================================================================
	// WHY THIS TEST EXISTS
	// =============================================================================
	// The router/coordinator stores assignment blobs in the internal topic.
	// If Encode/Decode changes subtly (e.g. empty input behavior), we can break
	// group sync or state rebuild.
	// =============================================================================

	in := []TopicPartition{
		{Topic: "orders", Partition: 0},
		{Topic: "orders", Partition: 1},
		{Topic: "payments", Partition: 2},
	}

	b, err := EncodeAssignment(in)
	if err != nil {
		t.Fatalf("EncodeAssignment failed: %v", err)
	}

	out, err := DecodeAssignment(b)
	if err != nil {
		t.Fatalf("DecodeAssignment failed: %v", err)
	}

	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round-trip mismatch:\n  in=%+v\n out=%+v", in, out)
	}
}

func TestAssignmentCodec_EmptyAndInvalid(t *testing.T) {
	// Empty input should decode to nil slice (not an error).
	got, err := DecodeAssignment(nil)
	if err != nil {
		t.Fatalf("DecodeAssignment(nil) failed: %v", err)
	}
	if got != nil {
		t.Fatalf("DecodeAssignment(nil) = %#v, want nil", got)
	}

	if _, err := DecodeAssignment([]byte("not-json")); err == nil {
		t.Fatalf("DecodeAssignment(invalid) = nil, want error")
	}
}
