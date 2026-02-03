package route

// Original https://github.com/kkdai/maglev
//
// Copyright (c) 2019 Evan Lin (github.com/kkdai)
//
// This program and the accompanying materials are made available under
// the terms of the Apache License, Version 2.0 which is available at
// http://www.apache.org/licenses/LICENSE-2.0.
//
// CHANGES:
// - Modified for integration with CF GoRouter
// - Added MaglevLookup interface for testability and abstraction
// - Enhanced with structured logging using slog
// - Added thread-safe operations
// - Extended with getter methods for unit testing
// - Added error handling and safety checks
// - Customized for hash-based routing requirements
// - Optimized memory: permutation values calculated on-the-fly (90% memory reduction)

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	// lookupTableSize is prime number for the size of the maglev lookup table, which should be approximately 100x
	// the number of expected endpoints
	lookupTableSize uint64 = 1801
)

// MaglevLookup defines the interface for consistent hashing lookup table implementations.
// This interface allows for different implementations of the Maglev algorithm and
// enables easy testing with mock implementations.
type MaglevLookup interface {
	// Add a new endpoint to the lookup table
	Add(endpoint string)

	// Remove an endpoint from the lookup table
	Remove(endpoint string)

	// GetInstanceForHashHeader endpoint by specified request header value
	GetInstanceForHashHeader(hashHeaderValue string) (uint64, string, error)

	// GetEndpointId returns the endpoint ID by specified lookup table index
	GetEndpointId(lookupTableIndex uint64) string

	// GetLookupTableSize returns the size of the lookup table
	GetLookupTableSize() uint64

	// GetEndpointList returns a copy of the current endpoint list (for testing)
	GetEndpointList() []string

	// GetLookupTable returns a copy of the current lookup table (for testing)
	GetLookupTable() []int
}

// Maglev implementation of consistent hashing algorithm described in "Maglev: A Fast and Reliable Software Network
// Load Balancer" (https://storage.googleapis.com/gweb-research2023-media/pubtools/2904.pdf)
// This implementation calculates permutation values on-the-fly instead of storing them, reducing memory by ~90%
type Maglev struct {
	logger       *slog.Logger
	lookupTable  []int
	endpointList []string
	lock         *sync.RWMutex
}

// NewMaglev initializes an empty maglev lookupTable table
func NewMaglev(logger *slog.Logger) *Maglev {
	return &Maglev{
		lock:         &sync.RWMutex{},
		lookupTable:  make([]int, lookupTableSize),
		endpointList: make([]string, 0, 2),
		logger:       logger,
	}
}

// Add a new endpoint to lookupTable if it's not already contained.
func (m *Maglev) Add(endpoint string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if lookupTableSize == uint64(len(m.endpointList)) {
		m.logger.Warn("maglev-add-lookuptable-capacity-exceeded", slog.String("endpoint-id", endpoint))
		return
	}

	index := sort.SearchStrings(m.endpointList, endpoint)
	if index < len(m.endpointList) && m.endpointList[index] == endpoint {
		m.logger.Debug("maglev-add-lookuptable-endpoint-exists", slog.String("endpoint-id", endpoint), slog.Int("current-endpoints", len(m.endpointList)))
		return
	}

	m.endpointList = append(m.endpointList, "")
	copy(m.endpointList[index+1:], m.endpointList[index:])
	m.endpointList[index] = endpoint

	m.fillLookupTable()
}

// Remove an endpoint from lookupTable if it's contained.
func (m *Maglev) Remove(endpoint string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	index := sort.SearchStrings(m.endpointList, endpoint)
	if index >= len(m.endpointList) || m.endpointList[index] != endpoint {
		m.logger.Debug("maglev-remove-endpoint-not-found", slog.String("endpoint-id", endpoint))
		return
	}

	m.endpointList = append(m.endpointList[:index], m.endpointList[index+1:]...)

	m.fillLookupTable()
}

func (m *Maglev) hashKey(headerValue string) uint64 {
	return m.calculateFNVHash64(headerValue)
}

// GetInstanceForHashHeader lookup table index and private instance ID for the specified request header value
func (m *Maglev) GetInstanceForHashHeader(hashHeaderValue string) (uint64, string, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if len(m.endpointList) == 0 {
		return 0, "", errors.New("no endpoint available")
	}
	key := m.hashKey(hashHeaderValue)
	index := key % lookupTableSize
	return index, m.endpointList[m.lookupTable[key%lookupTableSize]], nil
}

// GetEndpointId by specified lookup table index
func (m *Maglev) GetEndpointId(lookupTableIndex uint64) string {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.endpointList[m.lookupTable[lookupTableIndex]]
}

// calculatePermutationValue computes a single permutation value on-the-fly
// This replaces the stored permutation table with a calculation
func (m *Maglev) calculatePermutationValue(endpoint string, j uint64) uint64 {
	endpointHash := m.calculateFNVHash64(endpoint)
	offset := endpointHash % lookupTableSize
	skip := (endpointHash % (lookupTableSize - 1)) + 1
	return (offset + j*skip) % lookupTableSize
}

func (m *Maglev) fillLookupTable() {
	if len(m.endpointList) == 0 {
		return
	}

	numberOfEndpoints := len(m.endpointList)
	next := make([]int, numberOfEndpoints)
	entry := make([]int, lookupTableSize)
	for j := range entry {
		entry[j] = -1
	}

	for n := uint64(0); n <= lookupTableSize; {
		for i := 0; i < numberOfEndpoints; i++ {
			candidate := m.findNextAvailableSlot(i, next, entry)
			entry[candidate] = int(i)
			next[i] = next[i] + 1
			n++

			if n == lookupTableSize {
				m.lookupTable = entry
				return
			}
		}
	}
}

func (m *Maglev) findNextAvailableSlot(i int, next []int, entry []int) uint64 {
	// Calculate permutation on-the-fly instead of looking it up
	candidate := m.calculatePermutationValue(m.endpointList[i], uint64(next[i]))
	for entry[candidate] >= 0 {
		next[i]++
		if next[i] >= int(lookupTableSize) {
			// This should not happen in a properly functioning Maglev algorithm,
			// but we add this safety check to prevent panic
			m.logger.Error("maglev-permutation-exhausted",
				slog.Int("endpoint-index", i),
				slog.Int("next-value", next[i]))
			// Reset to beginning of permutation table as fallback
			next[i] = 0
		}
		candidate = m.calculatePermutationValue(m.endpointList[i], uint64(next[i]))
	}
	return candidate
}

// Getters for unit tests
func (m *Maglev) GetEndpointList() []string {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return append([]string(nil), m.endpointList...)
}

func (m *Maglev) GetLookupTable() []int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return append([]int(nil), m.lookupTable...)
}

func (m *Maglev) GetLookupTableSize() uint64 {
	return lookupTableSize
}

// TODO: Remove in final version
func (m *Maglev) PrintLookupTable() string {
	strArr := make([]string, len(m.lookupTable))
	for i, value := range m.lookupTable {
		strArr[i] = strconv.Itoa(value)
	}
	return fmt.Sprintf("[%s]", strings.Join(strArr, ", "))
}

// calculateFNVHash64 computes a hash using the non-cryptographic FNV hash algorithm.
func (m *Maglev) calculateFNVHash64(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

// Compile-time check to ensure Maglev implements MaglevLookup interface
var _ MaglevLookup = (*Maglev)(nil)
