package route

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	bigM uint64 = 293
)

type Maglev struct {
	logger            *slog.Logger
	permutation       [][]uint64
	lookupTable       []int64
	lookupTableSize   uint64
	endpointList      []string
	numberOfEndpoints uint64
	lock              *sync.RWMutex
}

// NewMaglev initializes an empty maglev lookupTable table
func NewMaglev(logger *slog.Logger) *Maglev {
	return &Maglev{
		lookupTableSize: bigM,
		lock:            &sync.RWMutex{},
		lookupTable:     make([]int64, bigM),
		endpointList:    make([]string, 0, 2),
		logger:          logger,
	}
}

// Add a new endpoint to maglev lookupTable table. Do nothing if the backend has been already added
func (m *Maglev) Add(endpoint string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, e := range m.endpointList {
		if e == endpoint {
			m.logger.Debug("endpoint already exists in lookupTable table", slog.String("endpoint-id", endpoint), slog.Int("current_endpoints", len(m.endpointList)))
			return
		}
	}

	if m.lookupTableSize == m.numberOfEndpoints {
		m.logger.Warn("Number of endpoints would exceed lookupTable table capacity")
		return
	}

	m.endpointList = append(m.endpointList, endpoint)
	m.numberOfEndpoints = uint64(len(m.endpointList))
	m.generatePermutation()
	m.populate()
	m.logger.Debug("endpoint added", slog.String("endpoint-id", endpoint), slog.String("lookupTable", m.PrintLookupTable()))
}

// Remove an endpoint from the lookupTable table. Returns an error if the backend was not found.
func (m *Maglev) Remove(endpoint string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	index := sort.SearchStrings(m.endpointList, endpoint)
	if index == len(m.endpointList) {
		// not found
		return
	}

	m.endpointList = append(m.endpointList[:index], m.endpointList[index+1:]...)

	m.numberOfEndpoints = uint64(len(m.endpointList))
	m.generatePermutation()
	m.populate()
}

func (m *Maglev) Clear() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.endpointList = nil
	m.permutation = nil
	m.lookupTable = nil
}

// Get endpoint by specified request header value
func (m *Maglev) Get(value string) (string, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if len(m.endpointList) == 0 {
		return "", errors.New("no endpoint available")
	}
	key := m.hashKey(value)
	return m.endpointList[m.lookupTable[key%m.lookupTableSize]], nil
}

func (m *Maglev) hashKey(obj string) uint64 {
	return m.calculateFNVHash64(obj)
}

// generatePermutation creates a permutation of the lookup table for each endpoint
func (m *Maglev) generatePermutation() {
	m.permutation = nil
	if len(m.endpointList) == 0 {
		return
	}
	m.permutation = make([][]uint64, len(m.endpointList))
	slices.Sort(m.endpointList)

	for i := 0; i < len(m.endpointList); i++ {
		endpoint := m.endpointList[i]

		offset := m.calculateFNVHash64(endpoint) % m.lookupTableSize
		skip := (m.calculateFNVHash64(endpoint) % (m.lookupTableSize - 1)) + 1

		permutationForEndpoint := make([]uint64, m.lookupTableSize)
		for j := uint64(0); j < m.lookupTableSize; j++ {
			permutationForEndpoint[j] = (offset + j*skip) % m.lookupTableSize
		}

		m.permutation[i] = permutationForEndpoint
	}
}

// populate fills lookupTable
func (m *Maglev) populate() {
	if len(m.endpointList) == 0 {
		return
	}

	var i uint64
	next := make([]uint64, m.numberOfEndpoints)
	entry := make([]int64, m.lookupTableSize)
	for j := range entry {
		entry[j] = -1
	}

	for n := uint64(0); n <= m.lookupTableSize; {
		for i = 0; i < m.numberOfEndpoints; i++ {
			candidate := m.findNextAvailableSlot(i, next, entry)
			entry[candidate] = int64(i)
			next[i] = next[i] + 1
			n++

			if n == m.lookupTableSize {
				m.lookupTable = entry
				return
			}
		}
	}
}

func (m *Maglev) findNextAvailableSlot(i uint64, next []uint64, entry []int64) uint64 {
	candidate := m.permutation[i][next[i]]
	for entry[candidate] >= 0 {
		next[i]++
		candidate = m.permutation[i][next[i]]
	}
	return candidate
}

func (m *Maglev) PrintLookupTable() string {
	strArr := make([]string, len(m.lookupTable))
	for i, value := range m.lookupTable {
		strArr[i] = strconv.FormatInt(value, 10)
	}
	return fmt.Sprintf("[%s]", strings.Join(strArr, ", "))
}

// calculateFNVHash64 computes a hash using the non-cryptographic FNV hash algorithm.
func (m *Maglev) calculateFNVHash64(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}
