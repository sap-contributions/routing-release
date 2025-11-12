package route

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	log "code.cloudfoundry.org/gorouter/logger"
)

// HashBased load balancing algorithm distributes requests based on a hash of a specific header value.
// The sticky session cookie has precedence over hash-based routing and the request should be routed to the instance stored in the cookie.
// If requests do not contain the hash-related header set configured for the hash-based route option, use the default load-balancing algorithm.
type HashBased struct {
	lock *sync.Mutex

	logger               *slog.Logger
	pool                 *EndpointPool
	lastEndpoint         *Endpoint
	lastLookupTableIndex uint64

	stickyEndpointID string
	mustBeSticky     bool

	HeaderValue string
}

// NewHashBased initializes an endpoint iterator that selects endpoints based on a hash of a header value.
// The global properties locallyOptimistic and localAvailabilityZone will be ignored when using Hash-Based Routing.
func NewHashBased(logger *slog.Logger, p *EndpointPool, initial string, mustBeSticky bool, locallyOptimistic bool, localAvailabilityZone string) EndpointIterator {
	return &HashBased{
		logger:           logger,
		pool:             p,
		lock:             &sync.Mutex{},
		stickyEndpointID: initial,
		mustBeSticky:     mustBeSticky,
	}
}

// Next selects the next endpoint based on the hash of the header value.
// If a sticky session endpoint is available and not overloaded, it will be returned.
// If the request must be sticky and the sticky endpoint is unavailable or overloaded, nil will be returned.
// If no sticky session is present, the endpoint will be selected based on the hash of the header value.
// It returns the same endpoint for the same header value consistently.
// If the hash lookup fails or the endpoint is not found, nil will be returned.
func (h *HashBased) Next(attempt int) *Endpoint {
	h.lock.Lock()
	defer h.lock.Unlock()

	endpoint := h.findEndpointIfStickySession()
	if endpoint == nil && h.mustBeSticky {
		return nil
	}

	if endpoint != nil {
		h.lastEndpoint = endpoint
		return endpoint
	}

	if len(h.pool.endpoints) == 0 {
		h.logger.Warn("hash-based-routing-pool-empty", slog.String("host", h.pool.host))
		return nil
	}

	endpoint = h.getSingleEndpoint()
	if endpoint != nil {
		h.lastEndpoint = endpoint
		return endpoint
	}

	if h.pool.HashLookupTable == nil {
		h.logger.Error("hash-based-routing-failed", slog.String("host", h.pool.host), log.ErrAttr(errors.New("Lookup table is empty")))
		return nil
	}

	if attempt == 0 || h.lastLookupTableIndex == 0 {
		initialLookupTableIndex, _, err := h.pool.HashLookupTable.GetInstanceForHashHeader(h.HeaderValue)

		if err != nil {
			h.logger.Error(
				"hash-based-routing-failed",
				slog.String("host", h.pool.host),
				log.ErrAttr(err),
			)
			return nil
		}

		endpoint = h.findEndpoint(initialLookupTableIndex, attempt)
	} else {
		// On retries, start looking from the next index in the lookup table
		nextIndex := (h.lastLookupTableIndex + 1) % h.pool.HashLookupTable.GetLookupTableSize()
		endpoint = h.findEndpoint(nextIndex, attempt)
	}

	if endpoint != nil {
		h.lastEndpoint = endpoint
	}
	return endpoint
}

func (h *HashBased) findEndpoint(index uint64, attempt int) *Endpoint {
	// Ensure we don't exceed the lookup table size
	lookupTableSize := h.pool.HashLookupTable.GetLookupTableSize()

	// Normalize index
	currentIndex := index % lookupTableSize
	// Keep track of endpoints already visited, to avoid visiting them twice
	visitedEndpoints := make(map[string]bool)

	numberOfEndpoints := len(h.pool.HashLookupTable.GetEndpointList())

	lastEndpointPrivateId := ""
	if attempt > 0 && h.lastEndpoint != nil {
		lastEndpointPrivateId = h.lastEndpoint.PrivateInstanceId
	}

	// abort when we have visited all available endpoints unsuccessfully
	for len(visitedEndpoints) < numberOfEndpoints {
		id := h.pool.HashLookupTable.GetEndpointId(currentIndex)

		if visitedEndpoints[id] || id == lastEndpointPrivateId {
			currentIndex = (currentIndex + 1) % lookupTableSize
			continue
		}
		visitedEndpoints[id] = true

		endpointElem := h.pool.findById(id)
		if endpointElem == nil {
			h.logger.Error("hash-based-routing-failed", slog.String("host", h.pool.host), log.ErrAttr(errors.New("Endpoint not found in pool")), slog.String("endpoint-id", id))
			currentIndex = (currentIndex + 1) % lookupTableSize
			continue
		}

		lastEndpointPrivateId = id

		if h.pool.HashRoutingProperties.BalanceFactor <= 0 || !h.isImbalancedOrOverloaded(endpointElem) {
			h.lastLookupTableIndex = currentIndex
			return endpointElem.endpoint
		}

		currentIndex = (currentIndex + 1) % lookupTableSize
	}
	// All endpoints checked and overloaded or not found
	h.logger.Error("hash-based-routing-failed", slog.String("host", h.pool.host), log.ErrAttr(errors.New("All endpoints are overloaded")))
	return nil
}

func (h *HashBased) isImbalancedOrOverloaded(e *endpointElem) bool {
	endpoint := e.endpoint
	return h.IsImbalancedOrOverloaded(endpoint, e.isOverloaded())
}

func (h *HashBased) IsImbalancedOrOverloaded(endpoint *Endpoint, isEndpointOverloaded bool) bool {
	avgNumberOfInFlightRequests := h.CalculateAverageLoad()
	currentInFlightRequestCount := endpoint.Stats.NumberConnections.Count()
	balanceFactor := h.pool.HashRoutingProperties.BalanceFactor

	if isEndpointOverloaded {
		h.logger.Debug("hash-based-routing-endpoint-overloaded", slog.String("host", h.pool.host), slog.String("endpoint-id", endpoint.PrivateInstanceId), slog.Int64("endpoint-connections", currentInFlightRequestCount))
		return true
	}

	// Check if avgNumberOfInFlightRequests is 0 to avoid division by 0
	if avgNumberOfInFlightRequests == 0 || float64(currentInFlightRequestCount)/avgNumberOfInFlightRequests > balanceFactor {
		h.logger.Debug("hash-based-routing-endpoint-imbalanced", slog.String("host", h.pool.host), slog.String("endpoint-id", endpoint.PrivateInstanceId), slog.Int64("endpoint-connections", endpoint.Stats.NumberConnections.Count()), slog.Float64("average-load", avgNumberOfInFlightRequests))
		return true
	}
	return false
}

// findEndpointIfStickySession checks if there is a sticky session endpoint and returns it if available.
// If the sticky session endpoint is overloaded, returns nil.
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

// EndpointFailed notifies the endpoint pool that the last selected endpoint has failed.
func (h *HashBased) EndpointFailed(err error) {
	if h.lastEndpoint != nil {
		h.pool.EndpointFailed(h.lastEndpoint, err)
	}
}

// PreRequest increments the in-flight request count for the selected endpoint from current Gorouter.
func (h *HashBased) PreRequest(e *Endpoint) {
	e.Stats.NumberConnections.Increment()
}

// PostRequest decrements the in-flight request count for the selected endpoint from current Gorouter.
func (h *HashBased) PostRequest(e *Endpoint) {
	e.Stats.NumberConnections.Decrement()
}

// CalculateAverageLoad computes the average number of in-flight requests across all endpoints in the pool.
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

func (h *HashBased) getSingleEndpoint() *Endpoint {
	if len(h.pool.endpoints) == 1 {
		e := h.pool.endpoints[0]
		if e.isOverloaded() {
			return nil
		}

		return e.endpoint
	}
	return nil
}
