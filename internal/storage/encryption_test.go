package storage

import (
	"crypto/rand"
	"encoding/hex"
	"testing"
)

// =============================================================================
// ENCRYPTION AT REST TESTS (M27)
// =============================================================================
//
// Tests cover:
//   - AES-256-GCM encryption/decryption round-trip
//   - Different payload sizes (empty, small, large)
//   - Ciphertext is different from plaintext (actually encrypted)
//   - Each encryption produces different ciphertext (random nonce)
//   - Invalid key handling
//   - Tampered ciphertext detection (GCM auth tag)
//   - NoopEncryptor passthrough behavior
//
// =============================================================================

// generateTestKey creates a valid 256-bit hex-encoded key for testing.
func generateTestKey(t *testing.T) string {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("failed to generate test key: %v", err)
	}
	return hex.EncodeToString(key)
}

func TestAESEncryptor_RoundTrip(t *testing.T) {
	key := generateTestKey(t)
	enc, err := NewAESEncryptor(key)
	if err != nil {
		t.Fatalf("NewAESEncryptor() error: %v", err)
	}

	tests := []struct {
		name      string
		plaintext []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("hello world")},
		{"medium", []byte("the quick brown fox jumps over the lazy dog")},
		{"binary", []byte{0x00, 0xFF, 0x01, 0xFE, 0x02, 0xFD}},
		{"large", make([]byte, 64*1024)}, // 64KB
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ciphertext, err := enc.Encrypt(tt.plaintext)
			if err != nil {
				t.Fatalf("Encrypt() error: %v", err)
			}

			// Ciphertext must be different from plaintext (unless empty)
			if len(tt.plaintext) > 0 && string(ciphertext) == string(tt.plaintext) {
				t.Error("ciphertext should differ from plaintext")
			}

			// Ciphertext must be longer (nonce + tag overhead)
			if len(tt.plaintext) > 0 && len(ciphertext) <= len(tt.plaintext) {
				t.Errorf("ciphertext (%d bytes) should be longer than plaintext (%d bytes)",
					len(ciphertext), len(tt.plaintext))
			}

			decrypted, err := enc.Decrypt(ciphertext)
			if err != nil {
				t.Fatalf("Decrypt() error: %v", err)
			}

			if string(decrypted) != string(tt.plaintext) {
				t.Errorf("round-trip failed: got %q, want %q", decrypted, tt.plaintext)
			}
		})
	}
}

func TestAESEncryptor_UniqueNonces(t *testing.T) {
	// Each encryption should produce different ciphertext due to random nonce
	key := generateTestKey(t)
	enc, err := NewAESEncryptor(key)
	if err != nil {
		t.Fatalf("NewAESEncryptor() error: %v", err)
	}

	plaintext := []byte("same message encrypted twice")
	ct1, _ := enc.Encrypt(plaintext)
	ct2, _ := enc.Encrypt(plaintext)

	if string(ct1) == string(ct2) {
		t.Error("two encryptions of the same plaintext should produce different ciphertext")
	}
}

func TestAESEncryptor_TamperedCiphertext(t *testing.T) {
	// GCM authentication tag should detect tampering
	key := generateTestKey(t)
	enc, err := NewAESEncryptor(key)
	if err != nil {
		t.Fatalf("NewAESEncryptor() error: %v", err)
	}

	plaintext := []byte("sensitive data")
	ciphertext, _ := enc.Encrypt(plaintext)

	// Flip a bit in the ciphertext (after the nonce)
	if len(ciphertext) > 13 {
		ciphertext[13] ^= 0xFF
	}

	_, err = enc.Decrypt(ciphertext)
	if err == nil {
		t.Error("Decrypt() should fail on tampered ciphertext")
	}
}

func TestAESEncryptor_WrongKey(t *testing.T) {
	key1 := generateTestKey(t)
	key2 := generateTestKey(t)

	enc1, _ := NewAESEncryptor(key1)
	enc2, _ := NewAESEncryptor(key2)

	plaintext := []byte("encrypted with key1")
	ciphertext, _ := enc1.Encrypt(plaintext)

	_, err := enc2.Decrypt(ciphertext)
	if err == nil {
		t.Error("Decrypt() should fail with wrong key")
	}
}

func TestAESEncryptor_InvalidKeys(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{"too_short", "abcd"},
		{"too_long", "aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaa"},
		{"not_hex", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"},
		{"empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewAESEncryptor(tt.key)
			if err == nil {
				t.Errorf("NewAESEncryptor(%q) should fail", tt.key)
			}
		})
	}
}

func TestAESEncryptor_IsEnabled(t *testing.T) {
	key := generateTestKey(t)
	enc, _ := NewAESEncryptor(key)

	if !enc.IsEnabled() {
		t.Error("AESEncryptor.IsEnabled() should return true")
	}
}

func TestNoopEncryptor(t *testing.T) {
	noop := &NoopEncryptor{}

	if noop.IsEnabled() {
		t.Error("NoopEncryptor.IsEnabled() should return false")
	}

	plaintext := []byte("pass through data")
	encrypted, err := noop.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt() error: %v", err)
	}
	if string(encrypted) != string(plaintext) {
		t.Error("NoopEncryptor.Encrypt() should return plaintext unchanged")
	}

	decrypted, err := noop.Decrypt(encrypted)
	if err != nil {
		t.Fatalf("Decrypt() error: %v", err)
	}
	if string(decrypted) != string(plaintext) {
		t.Error("NoopEncryptor.Decrypt() should return data unchanged")
	}
}

func TestAESEncryptor_DecryptTooShort(t *testing.T) {
	key := generateTestKey(t)
	enc, _ := NewAESEncryptor(key)

	// Ciphertext shorter than nonce size should fail
	_, err := enc.Decrypt([]byte("short"))
	if err == nil {
		t.Error("Decrypt() should fail on ciphertext shorter than nonce")
	}
}

func BenchmarkAESEncryptor_Encrypt(b *testing.B) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	enc, _ := NewAESEncryptor(hex.EncodeToString(key))
	plaintext := make([]byte, 1024) // 1KB message

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = enc.Encrypt(plaintext)
	}
}

func BenchmarkAESEncryptor_Decrypt(b *testing.B) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	enc, _ := NewAESEncryptor(hex.EncodeToString(key))
	plaintext := make([]byte, 1024)
	ciphertext, _ := enc.Encrypt(plaintext)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = enc.Decrypt(ciphertext)
	}
}
