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
	lookupTableSize uint64 //sie of the lookup table
	logger          *slog.Logger
	permutation     [][]uint64
	lookup          []int64
	backendList     []string
	lock            *sync.RWMutex
}

// NewMaglev :
func NewMaglev(logger *slog.Logger) *Maglev {
	mag := &Maglev{lookupTableSize: bigM, lock: &sync.RWMutex{}, lookup: make([]int64, bigM), logger: logger}
	return mag
}

// Add : Return nil if add success or backend has been added already, otherwise return error
func (m *Maglev) Add(backend string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, v := range m.backendList {
		if v == backend {
			return nil
		}
	}

	if m.lookupTableSize == m.noOfBackends {
		return errors.New("Number of backends would be greater than lookup table")
	}

	m.backendList = append(m.backendList, backend)
	m.noOfBackends = uint64(len(m.backendList))
	m.generatePopulation()
	m.populate()
	m.logger.Info("backend added", slog.String("backend", backend), slog.String("lookupTable", m.PrintLookupTable()))
	return nil
}

// Remove : removes a backend from the Maglev hash. Returns an error if the backend was not found.
func (m *Maglev) Remove(backend string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	index := sort.SearchStrings(m.backendList, backend)
	if index == len(m.backendList) {
		return errors.New("Not found")
	}

	m.backendList = append(m.backendList[:index], m.backendList[index+1:]...)

	m.noOfBackends = uint64(len(m.backendList))
	m.generatePopulation()
	m.populate()
	return nil
}

func (m *Maglev) Clear() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.backendList = nil
	m.permutation = nil
	m.lookup = nil
}

// Get :Get backend by object string.
func (m *Maglev) Get(obj string) (string, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if len(m.backendList) == 0 {
		return "", errors.New("Empty")
	}
	key := m.hashKey(obj)
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
			c := m.permutation[i][next[i]]
			for entry[c] >= 0 {
				next[i] = next[i] + 1
				c = m.permutation[i][next[i]]
			}

			entry[c] = int64(i)
			next[i] = next[i] + 1
			n++

			if n == m.lookupTableSize {
				m.lookup = entry
				return
			}
		}

	}

}

func (m *Maglev) PrintLookupTable() string {
	strArr := make([]string, len(m.lookup))
	for i, value := range m.lookup {
		strArr[i] = fmt.Sprintf("%d", value)
	}
	return "[" + strings.Join(strArr, ", ") + "]"
}

// CalculateHash computes a hash using the FNV hash algorithm.
func CalculateFNVHash64(key string) uint64 {
	h := fnv.New64a()    // Create a new FNV hash function (32-bit size)
	h.Write([]byte(key)) // Write the key into the hash function
	return h.Sum64()     // Retrieve the hash value as a uint64
}
