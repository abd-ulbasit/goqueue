// =============================================================================
// QUOTA MANAGER LOCKING REGRESSION TESTS
// =============================================================================
//
// Same hazard as Log.ReadFrom in internal/storage: a method takes qm.mu.RLock
// and then, still holding it, calls an *exported* sibling that takes qm.mu.RLock
// again. Go's sync.RWMutex gives waiting writers priority, so a single
// concurrent Lock queued between the two RLocks parks both goroutines forever.
//
// GetAllQuotaStats is the worse of the two cases: its inner call sits inside a
// loop over every tenant, so the vulnerable window scales with tenant count.
//
// =============================================================================

package broker

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestQuotaManager_GetAllQuotaStats_ConcurrentWriterDoesNotDeadlock pins the
// recursive read lock that GetAllQuotaStats used to take by calling the
// exported GetQuotaStats from inside its own read-locked loop.
//
// Readers loop over GetAllQuotaStats while writers repeatedly queue on
// qm.mu.Lock via InitializeTenant. One overlap wedges the manager permanently,
// so the stop signal is never observed and the WaitGroup never drains.
func TestQuotaManager_GetAllQuotaStats_ConcurrentWriterDoesNotDeadlock(t *testing.T) {
	qm := NewQuotaManager()

	// Seed tenants so the read-locked loop body actually runs.
	for i := 0; i < 32; i++ {
		qm.InitializeTenant(fmt.Sprintf("tenant-%d", i), DefaultTenantQuotas())
	}

	const (
		readers  = 8
		writers  = 2
		duration = 300 * time.Millisecond
	)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = qm.GetAllQuotaStats()
			}
		}()
	}

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				qm.InitializeTenant(fmt.Sprintf("w%d-tenant-%d", id, n%8), DefaultTenantQuotas())
			}
		}(i)
	}

	time.Sleep(duration)
	close(stop)

	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: GetAllQuotaStats re-entered qm.mu.RLock while a writer was queued")
	}
}

// TestQuotaManager_GetAllQuotaStats_MatchesPerTenant asserts the aggregate view
// still agrees with the single-tenant accessor, so the lock-free variant behind
// GetAllQuotaStats cannot silently drift from the exported one.
func TestQuotaManager_GetAllQuotaStats_MatchesPerTenant(t *testing.T) {
	qm := NewQuotaManager()

	tenants := []string{"alpha", "beta", "gamma"}
	for _, id := range tenants {
		qm.InitializeTenant(id, DefaultTenantQuotas())
	}

	all := qm.GetAllQuotaStats()
	if len(all) != len(tenants) {
		t.Fatalf("GetAllQuotaStats returned %d tenants, want %d", len(all), len(tenants))
	}

	for _, id := range tenants {
		want, err := qm.GetQuotaStats(id)
		if err != nil {
			t.Fatalf("GetQuotaStats(%q) failed: %v", id, err)
		}
		got, ok := all[id]
		if !ok {
			t.Fatalf("GetAllQuotaStats missing tenant %q", id)
		}
		if got.TenantID != want.TenantID {
			t.Errorf("tenant %q: TenantID = %q, want %q", id, got.TenantID, want.TenantID)
		}
		if got.Quotas != want.Quotas {
			t.Errorf("tenant %q: Quotas mismatch between aggregate and per-tenant view", id)
		}
	}
}
