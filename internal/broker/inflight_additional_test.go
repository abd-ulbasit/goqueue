package broker

import "testing"

func TestInFlightState_String(t *testing.T) {
	// Cover all known states plus an unknown value.
	cases := []struct {
		state InFlightState
		want  string
	}{
		{StateInFlight, "InFlight"},
		{StateScheduledRetry, "ScheduledRetry"},
		{StateAcked, "Acked"},
		{StateRejected, "Rejected"},
		{InFlightState(999), "Unknown"},
	}

	for _, tc := range cases {
		if got := tc.state.String(); got != tc.want {
			t.Fatalf("state %v String()=%q, want %q", tc.state, got, tc.want)
		}
	}
}

func TestParseReceiptHandle_RoundTripAndErrors(t *testing.T) {
	// A deterministic handle (we don't need randomness from GenerateReceiptHandle
	// to test the parser logic).
	h := "orders:2:123:4:deadbeef"
	parsed, err := ParseReceiptHandle(h)
	if err != nil {
		t.Fatalf("ParseReceiptHandle failed: %v", err)
	}
	if parsed.Topic != "orders" || parsed.Partition != 2 || parsed.Offset != 123 || parsed.DeliveryCount != 4 || parsed.Nonce != "deadbeef" {
		t.Fatalf("ParseReceiptHandle parsed=%+v, want orders/2/123/4/deadbeef", parsed)
	}

	// Wrong part count.
	if _, err := ParseReceiptHandle("too:few:parts"); err == nil {
		t.Fatalf("ParseReceiptHandle expected error for wrong part count")
	}

	// Invalid ints.
	if _, err := ParseReceiptHandle("orders:x:123:4:deadbeef"); err == nil {
		t.Fatalf("ParseReceiptHandle expected error for invalid partition")
	}
	if _, err := ParseReceiptHandle("orders:2:x:4:deadbeef"); err == nil {
		t.Fatalf("ParseReceiptHandle expected error for invalid offset")
	}
	if _, err := ParseReceiptHandle("orders:2:123:x:deadbeef"); err == nil {
		t.Fatalf("ParseReceiptHandle expected error for invalid delivery count")
	}
	// Empty nonce isn't validated today; ensure the parser still succeeds.
	if parsed, err := ParseReceiptHandle("orders:2:123:4:"); err != nil || parsed.Nonce != "" {
		t.Fatalf("ParseReceiptHandle empty nonce parsed=%+v err=%v, want nonce=\"\" and nil err", parsed, err)
	}
}
