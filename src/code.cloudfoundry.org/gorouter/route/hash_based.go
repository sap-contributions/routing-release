package route

import (
	"log/slog"
	"sync"
)

type HashBased struct {
	logger *slog.Logger
	pool   *EndpointPool
	lock   *sync.Mutex

	initialEndpoint       string
	mustBeSticky          bool
	locallyOptimistic     bool
	localAvailabilityZone string

	HeaderValue string
}

func NewHashBased(logger *slog.Logger, p *EndpointPool, initial string, mustBeSticky bool, locallyOptimistic bool, localAvailabilityZone string) EndpointIterator {
	return &HashBased{
		logger:                logger,
		pool:                  p,
		lock:                  &sync.Mutex{},
		initialEndpoint:       initial,
		mustBeSticky:          mustBeSticky,
		locallyOptimistic:     locallyOptimistic,
		localAvailabilityZone: localAvailabilityZone,
	}
}

func (h *HashBased) Next(attempt int) *Endpoint {
	h.lock.Lock()
	defer h.lock.Unlock()

	if h.pool.HashLookupTable == nil {
		h.logger.Error("Hash-based lookup table is empty")
		return nil
	}

	// Now we can use h.HeaderValue to determine the endpoint
	id, error := h.pool.HashLookupTable.Get(h.HeaderValue)
	h.logger.Info("Lookup for hash value", slog.String("Value:", h.HeaderValue), slog.String("backend ID:", id))
	if error != nil {
		h.logger.Error("failed to get Next")
	}

	e := h.pool.findById(id)
	if e == nil {
		h.logger.Error("not found")
		return nil
	}
	return e.endpoint
}

func (h *HashBased) EndpointFailed(err error) {
	//TODO implement me
	panic("implement me")
}

func (h *HashBased) PreRequest(e *Endpoint) {
	e.Stats.NumberConnections.Increment()
}

func (h *HashBased) PostRequest(e *Endpoint) {
	e.Stats.NumberConnections.Decrement()
}
