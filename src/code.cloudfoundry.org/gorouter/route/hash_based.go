package route

import (
	"context"
	"log/slog"
	"sync"
)

type HashBased struct {
	lock *sync.Mutex

	logger       *slog.Logger
	pool         *EndpointPool
	lastEndpoint *Endpoint

	stickyEndpointID string
	mustBeSticky     bool

	HeaderValue string
}

func NewHashBased(logger *slog.Logger, p *EndpointPool, initial string, mustBeSticky bool, locallyOptimistic bool, localAvailabilityZone string) EndpointIterator {
	return &HashBased{
		logger:           logger,
		pool:             p,
		lock:             &sync.Mutex{},
		stickyEndpointID: initial,
		mustBeSticky:     mustBeSticky,
	}
}

func (h *HashBased) Next(attempt int) *Endpoint {
	h.lock.Lock()
	defer h.lock.Unlock()

	e := h.findEndpointIfStickySession()
	if e == nil && h.mustBeSticky {
		return nil
	}

	if e != nil {
		h.lastEndpoint = e
		return e
	}

	if h.pool.HashLookupTable == nil {
		h.logger.Error("hash-based-routing-failed", slog.String("host", h.pool.host), slog.String("error", "Lookup table is empty"))
		return nil
	}

	// Now we can use h.HeaderValue to determine the endpoint
	id, error := h.pool.HashLookupTable.Get(h.HeaderValue)
	h.logger.Info("hash-based-routing", slog.String("hash header value", h.HeaderValue), slog.String("endpoint-id", id))

	if error != nil {
		h.logger.Error("hash-based-routing-failed", slog.String("host", h.pool.host), slog.String("error", "No endpoints in lookup table"))
	}

	endpointElem := h.pool.findById(id)
	if endpointElem == nil {
		h.logger.Error("hash-based-routing-failed", slog.String("host", h.pool.host), slog.String("error", "Endpoint not found in pool"), slog.String("endpoint-id", id))

		return nil
	}
	return endpointElem.endpoint
}

func (h *HashBased) findEndpointIfStickySession() *Endpoint {
	var e *endpointElem
	if h.stickyEndpointID != "" {
		e = h.pool.findById(h.stickyEndpointID)
		if e != nil && e.isOverloaded() {
			if h.mustBeSticky {
				if h.logger.Enabled(context.Background(), slog.LevelDebug) {
					h.logger.Debug("endpoint-overloaded-but-request-must-be-sticky", e.endpoint.ToLogData()...)
				}
				return nil
			}
			e = nil
		}

		if e == nil && h.mustBeSticky {
			h.logger.Debug("endpoint-missing-but-request-must-be-sticky", slog.String("requested-endpoint", h.stickyEndpointID))
			return nil
		}

		if !h.mustBeSticky {
			h.logger.Debug("endpoint-missing-choosing-alternate", slog.String("requested-endpoint", h.stickyEndpointID))
			h.stickyEndpointID = ""
		}
	}

	if e != nil {
		e.RLock()
		defer e.RUnlock()
		return e.endpoint
	}
	return nil
}

func (h *HashBased) EndpointFailed(err error) {
	if h.lastEndpoint != nil {
		h.pool.EndpointFailed(h.lastEndpoint, err)
	}
}

func (h *HashBased) PreRequest(e *Endpoint) {
	e.Stats.NumberConnections.Increment()
}

func (h *HashBased) PostRequest(e *Endpoint) {
	e.Stats.NumberConnections.Decrement()
}
