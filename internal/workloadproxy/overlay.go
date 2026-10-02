package workloadproxy

import (
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/openziti/sdk-golang/ziti"
)

// RequireNoFallback refuses an SDK dialer that could reach a destination
// outside the overlay. sdk-golang's collection dialer keeps its fallback in an
// unexported field (CtxCollection.NewDialerWithFallback sets it to a plain
// net.Dialer); only NewDialer leaves it nil. Checked at startup, so a future
// change to the constructor fails the sidecar instead of opening egress.
func RequireNoFallback(dialer any) error {
	value := reflect.ValueOf(dialer)
	for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
		if value.IsNil() {
			return errors.New("overlay dialer is nil")
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return errors.New("overlay dialer has an unknown shape")
	}
	fallback := value.FieldByName("fallback")
	if !fallback.IsValid() {
		return errors.New("overlay dialer has no inspectable fallback")
	}
	if (fallback.Kind() == reflect.Interface || fallback.Kind() == reflect.Pointer) && fallback.IsNil() {
		return nil
	}
	return errors.New("overlay dialer has a non-overlay fallback")
}

// minRefreshInterval bounds on-miss service refreshes across all clients.
const minRefreshInterval = 2 * time.Second

// ContextClassifier classifies destinations by the identity's own intercept
// list, the same match the collection dialer uses to pick a service.
type ContextClassifier struct {
	Context ziti.Context
	mu      sync.Mutex
	last    time.Time
}

func (c *ContextClassifier) Intercepted(host string, port uint16) bool {
	_, _, err := c.Context.GetServiceForAddr("tcp", host, port)
	return err == nil
}

func (c *ContextClassifier) Refresh() {
	c.mu.Lock()
	if time.Since(c.last) < minRefreshInterval {
		c.mu.Unlock()
		return
	}
	c.last = time.Now()
	c.mu.Unlock()
	_ = c.Context.RefreshServices()
}
