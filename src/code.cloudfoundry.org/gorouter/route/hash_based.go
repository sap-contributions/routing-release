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
	lastEndpoint          *Endpoint
	locallyOptimistic     bool
	localAvailabilityZone string

	nextIdx     int
	HeaderValue string
}

func NewHashBased(logger *slog.Logger, p *EndpointPool, initial string, mustBeSticky bool, locallyOptimistic bool, localAvailabilityZone string) EndpointIterator {
	return &HashBased{
		logger:                logger,
		pool:                  p,
		initialEndpoint:       initial,
		mustBeSticky:          mustBeSticky,
		locallyOptimistic:     locallyOptimistic,
		localAvailabilityZone: localAvailabilityZone,
	}
}

func (h *HashBased) Next(attempt int) *Endpoint {
	// Now we can use h.HeaderValue to determine the endpoint

	//TODO implement me
	panic("implement me")
}

func (h *HashBased) EndpointFailed(err error) {
	//TODO implement me
	panic("implement me")
}

func (h *HashBased) PreRequest(e *Endpoint) {
	//TODO implement me
	panic("implement me")
}

func (h *HashBased) PostRequest(e *Endpoint) {
	//TODO implement me
	panic("implement me")
}
