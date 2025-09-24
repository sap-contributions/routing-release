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
	n           uint64 //size of VIP backends
	m           uint64 //sie of the lookup table
	logger      *slog.Logger
	permutation [][]uint64
	lookup      []int64
	nodeList    []string
	lock        *sync.RWMutex
}

// NewMaglev :
func NewMaglev(logger *slog.Logger) *Maglev {
	mag := &Maglev{m: bigM, lock: &sync.RWMutex{}, lookup: make([]int64, bigM), logger: logger}
	return mag
}

// Add : Return nil if add success, otherwise return error
func (m *Maglev) Add(backend string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	for _, v := range m.nodeList {
		if v == backend {
			return errors.New("Exist already")
		}
	}

	if m.m == m.n {
		return errors.New("Number of backends would be greater than lookup table")
	}

	m.nodeList = append(m.nodeList, backend)
	m.n = uint64(len(m.nodeList))
	m.generatePopulation()
	m.populate()
	m.logger.Info("backend added", slog.String("backend", backend), slog.String("lookupTable", m.PrintLookupTable()))
	return nil
}

// Remove :
func (m *Maglev) Remove(backend string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	index := sort.SearchStrings(m.nodeList, backend)
	if index == len(m.nodeList) {
		return errors.New("Not found")
	}

	m.nodeList = append(m.nodeList[:index], m.nodeList[index+1:]...)

	m.n = uint64(len(m.nodeList))
	m.generatePopulation()
	m.populate()
	return nil
}

func (m *Maglev) Clear() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.nodeList = nil
	m.permutation = nil
	m.lookup = nil
}

// Get :Get node name by object string.
func (m *Maglev) Get(obj string) (string, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	if len(m.nodeList) == 0 {
		return "", errors.New("Empty")
	}
	key := m.hashKey(obj)
	return m.nodeList[m.lookup[key%m.m]], nil
}

func (m *Maglev) hashKey(obj string) uint64 {
	return CalculateFNVHash64(obj)
}

func (m *Maglev) generatePopulation() {
	m.permutation = nil
	if len(m.nodeList) == 0 {
		return
	}

	sort.Strings(m.nodeList)

	for i := 0; i < len(m.nodeList); i++ {
		bData := m.nodeList[i]

		offset := CalculateFNVHash64(bData) % m.m
		skip := (CalculateFNVHash64(bData) % (m.m - 1)) + 1

		iRow := make([]uint64, m.m)
		var j uint64
		for j = 0; j < m.m; j++ {
			iRow[j] = (offset + uint64(j)*skip) % m.m
		}

		m.permutation = append(m.permutation, iRow)
	}
}

func (m *Maglev) populate() {
	if len(m.nodeList) == 0 {
		return
	}

	var i, j uint64
	next := make([]uint64, m.n)
	entry := make([]int64, m.m)
	for j = 0; j < m.m; j++ {
		entry[j] = -1
	}

	var n uint64

	for { //true
		for i = 0; i < m.n; i++ {
			c := m.permutation[i][next[i]]
			for entry[c] >= 0 {
				next[i] = next[i] + 1
				c = m.permutation[i][next[i]]
			}

			entry[c] = int64(i)
			next[i] = next[i] + 1
			n++

			if n == m.m {
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
