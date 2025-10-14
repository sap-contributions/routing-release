package route

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"
	"strings"
	"sync"
)

const (
	// bigM uint64 = 65537
	bigM uint64 = 293
)

// Maglev :
type Maglev struct {
	noOfBackends    uint64 //size of VIP backends
	lookupTableSize uint64
	logger          *slog.Logger
	permutation     [][]uint64
	lookup          []int64
	backendList     []string
	lock            *sync.RWMutex
}

// NewMaglev initializes an empty maglev lookup table
func NewMaglev(logger *slog.Logger) *Maglev {
	mag := &Maglev{lookupTableSize: bigM, lock: &sync.RWMutex{}, lookup: make([]int64, bigM), backendList: make([]string, 0, 10), logger: logger}
	return mag
}

// Add a new endpoint to maglev lookup table. Do nothing if the backend has been already added
func (m *Maglev) Add(backend string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, v := range m.backendList {
		if v == backend {
			m.logger.Debug("backend already exists in lookup table", slog.String("endpoint-id", backend), slog.Int("current_backends", len(m.backendList)))
			return
		}
	}

	if m.lookupTableSize == m.noOfBackends {
		m.logger.Warn("Number of backends would exceed lookup table capacity")
		return
	}

	m.backendList = append(m.backendList, backend)
	m.noOfBackends = uint64(len(m.backendList))
	m.generatePopulation()
	m.populate()
	m.logger.Debug("backend added", slog.String("endpoint-id", backend), slog.String("lookupTable", m.PrintLookupTable()))
}

// Remove an endpoint from the lookup table. Returns an error if the backend was not found.
func (m *Maglev) Remove(backend string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	index := sort.SearchStrings(m.backendList, backend)
	if index == len(m.backendList) {
		// not found
		return
	}

	m.backendList = append(m.backendList[:index], m.backendList[index+1:]...)

	m.noOfBackends = uint64(len(m.backendList))
	m.generatePopulation()
	m.populate()
}

func (m *Maglev) Clear() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.backendList = nil
	m.permutation = nil
	m.lookup = nil
}

// Get endpoint by specified request header value
func (m *Maglev) Get(value string) (string, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if len(m.backendList) == 0 {
		return "", errors.New("does not exist")
	}
	key := m.hashKey(value)
	return m.backendList[m.lookup[key%m.lookupTableSize]], nil
}

func (m *Maglev) hashKey(obj string) uint64 {
	return CalculateFNVHash64(obj)
}

func (m *Maglev) generatePopulation() {
	m.permutation = nil
	if len(m.backendList) == 0 {
		return
	}

	sort.Strings(m.backendList)

	for i := 0; i < len(m.backendList); i++ {
		bData := m.backendList[i]

		offset := CalculateFNVHash64(bData) % m.lookupTableSize
		skip := (CalculateFNVHash64(bData) % (m.lookupTableSize - 1)) + 1

		iRow := make([]uint64, m.lookupTableSize)
		var j uint64
		for j = 0; j < m.lookupTableSize; j++ {
			iRow[j] = (offset + uint64(j)*skip) % m.lookupTableSize
		}

		m.permutation = append(m.permutation, iRow)
	}
}

func (m *Maglev) populate() {
	if len(m.backendList) == 0 {
		return
	}

	var i, j uint64
	next := make([]uint64, m.noOfBackends)
	entry := make([]int64, m.lookupTableSize)
	for j = 0; j < m.lookupTableSize; j++ {
		entry[j] = -1
	}

	var n uint64

	for { //true
		for i = 0; i < m.noOfBackends; i++ {
			candidate := m.findNextAvailableSlot(i, next, entry)
			entry[candidate] = int64(i)
			next[i] = next[i] + 1
			n++

			if n == m.lookupTableSize {
				m.lookup = entry
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
	strArr := make([]string, len(m.lookup))
	for i, value := range m.lookup {
		strArr[i] = fmt.Sprintf("%d", value)
	}
	return "[" + strings.Join(strArr, ", ") + "]"
}

// CalculateFNVHash64 computes a hash using the non-cryptographic FNV hash algorithm.
func CalculateFNVHash64(key string) uint64 {
	// TODO: initialize a hash function only once per table
	h := fnv.New64a()    // Create a new FNV hash function
	h.Write([]byte(key)) // Write the key into the hash function
	return h.Sum64()     // Retrieve the hash value
}
