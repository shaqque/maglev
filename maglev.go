package maglev

import (
	"errors"
	"math/big"
	"sort"
)

// Hasher hashes strings to uint64.
type Hasher interface {
	Hash(string) uint64
}

// Maglev contains
type Maglev struct {
	permutations  map[string][]uint64
	lookup        []string
	nodes         []string
	numPartitions uint64
	h1, h2        Hasher
}

// NewMaglev initializes a Maglev hasher.
func NewMaglev(nodes []string, numPartitions uint64, h1, h2 Hasher) (*Maglev, error) {
	// check if numPartitions is prime
	if !big.NewInt(0).SetUint64(numPartitions).ProbablyPrime(0) {
		return nil, errors.New("number of partitions must be prime")
	}

	nodescopy := make([]string, len(nodes))
	copy(nodescopy, nodes)
	sort.Strings(nodescopy)

	m := &Maglev{
		nodes:         nodescopy,
		numPartitions: numPartitions,
		h1:            h1,
		h2:            h2,
	}
	if len(nodes) > 0 {
		m.generatePermutations()
		m.populateLookup()
	}

	return m, nil
}

func (m *Maglev) generatePermutations() {
	m.permutations = make(map[string][]uint64)
	for _, node := range m.nodes {
		m.permutations[node] = m.generatePermutationsForNode(node)
	}
}

func (m *Maglev) generatePermutationsForNode(node string) []uint64 {
	offset := m.h1.Hash(node) % m.numPartitions
	skip := m.h2.Hash(node)%(m.numPartitions-1) + 1

	permutation := make([]uint64, m.numPartitions)
	for i := uint64(0); i < m.numPartitions; i++ {
		permutation[i] = (offset + i*skip) % m.numPartitions
	}
	return permutation
}

func (m *Maglev) populateLookup() {
	N := len(m.nodes)
	if N == 0 {
		panic("cannot populate lookup table without nodes")
	}
	m.lookup = make([]string, m.numPartitions)
	next := make([]int, N)
	var n uint64
	for {
		for i, ID := range m.nodes {
			c := m.permutations[ID][next[i]]
			for m.lookup[c] != "" {
				next[i]++
				c = m.permutations[ID][next[i]]
			}
			m.lookup[c] = ID
			next[i]++
			n++
			if n == m.numPartitions {
				return
			}
		}
	}
}

// Lookup returns the node the key belongs to.
func (m *Maglev) Lookup(key uint64) string {
	partitionID := m.PartitionID(key)
	return m.lookup[partitionID]
}

// PartitionID returns the partition the key belongs to.
func (m *Maglev) PartitionID(key uint64) int {
	return int(key % uint64(m.numPartitions))
}

// Contains returns true if Maglev contains the node.
func (m *Maglev) Contains(node string) bool {
	// binary search
	if pos := sort.SearchStrings(m.nodes, node); m.nodes[pos] == node {
		return true
	}
	return false
}

// Add adds new nodes to Maglev and returns the number of nodes added.
func (m *Maglev) Add(nodes ...string) (int, error) {
	n := 0
	for _, node := range nodes {
		// check if node doesn't exist yet
		if pos := sort.SearchStrings(m.nodes, node); m.nodes[pos] != node {
			// insert node
			m.nodes = append(m.nodes[:pos], append([]string{node}, m.nodes[pos:]...)...)
			m.permutations[node] = m.generatePermutationsForNode(node)
			n++
		}
	}
	m.populateLookup()
	return n, nil
}

// Remove removes nodes from Maglev and returns the number of nodes removed.
func (m *Maglev) Remove(nodes ...string) (int, error) {
	n := 0
	for _, node := range nodes {
		// check if node really exists
		if pos := sort.SearchStrings(m.nodes, node); m.nodes[pos] == node {
			// delete node
			m.nodes = append(m.nodes[:pos], m.nodes[pos+1:]...)
			delete(m.permutations, node)
			n++
		}
	}
	m.populateLookup()
	return n, nil
}
