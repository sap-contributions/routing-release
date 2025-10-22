package route

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	log "code.cloudfoundry.org/gorouter/logger"
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
		h.logger.Error("hash-based-routing-failed", slog.String("host", h.pool.host), log.ErrAttr(errors.New("Lookup table is empty")))
		return nil
	}

	index, err := h.pool.HashLookupTable.GetLookupTableIndex(h.HeaderValue)

	if err != nil {
		h.logger.Error(
			"hash-based-routing-failed",
			slog.String("host", h.pool.host),
			log.ErrAttr(err),
		)
		return nil
	}

	e = h.findEndpoint(index)

	return e
}

func (h *HashBased) findEndpoint(index uint64) *Endpoint {
	maxAttempts := len(h.pool.endpoints)
	if maxAttempts == 0 {
		return nil
	}

	lastEndpointPrivateId := ""
	for attempt := 0; attempt < maxAttempts; attempt++ {

		id := h.pool.HashLookupTable.GetEndpointId(index)

		if id == lastEndpointPrivateId {
			index++
			continue
		}

		h.logger.Debug(
			"hash-based-routing",
			slog.String("hash header value", h.HeaderValue),
			slog.String("endpoint-id", id),
		)

		endpointElem := h.pool.findById(id)
		if endpointElem == nil {
			h.logger.Error("hash-based-routing-failed", slog.String("host", h.pool.host), log.ErrAttr(errors.New("Endpoint not found in pool")), slog.String("endpoint-id", id))
			return nil
		}

		e := endpointElem.endpoint
		if h.pool.HashRoutingProperties.BalanceFactor <= 0 {
			return e
		}

		lastEndpointPrivateId = id

		if !h.checkOverloadSituation(e) {
			return e
		}

		index = (index + 1) % h.pool.HashLookupTable.lookupTableSize
	}
	// All endpoints checked and overloaded
	h.logger.Error("hash-based-routing-failed", slog.String("host", h.pool.host), log.ErrAttr(errors.New("All endpoints are overloaded")))
	return nil
}

func (h *HashBased) checkOverloadSituation(e *Endpoint) bool {
	avgLoad := h.CalculateAverageLoad()
	balanceFactor := h.pool.HashRoutingProperties.BalanceFactor
	if float64(e.Stats.NumberConnections.Count())/avgLoad > balanceFactor {
		h.logger.Info("hash-based-routing-endpoint-overloaded", slog.String("host", h.pool.host), slog.String("endpoint-id", e.PrivateInstanceId), slog.Int64("endpoint-connections", e.Stats.NumberConnections.Count()), slog.Float64("average-load", avgLoad))
		return true
	}
	return false
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

func (h *HashBased) CalculateAverageLoad() float64 {
	if len(h.pool.endpoints) == 0 {
		return 0
	}

	var currentInFlightRequestCount int64
	for _, endpointElem := range h.pool.endpoints {
		endpointElem.RLock()
		currentInFlightRequestCount += endpointElem.endpoint.Stats.NumberConnections.Count()
		endpointElem.RUnlock()
	}

	return float64(currentInFlightRequestCount) / float64(len(h.pool.endpoints))
}
