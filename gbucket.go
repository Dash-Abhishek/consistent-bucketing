package gbucket

import (
	"errors"
	"fmt"

	"github.com/spaolacci/murmur3"
)

const (
	seed  = 1000
	slots = 1000
)

type Bucket struct {
	Percentage float64
	Bucketname string
}

type Allocations struct {
	Mappings []Allocation
}

type Allocation struct {
	Name       string
	Percentage float64
	MinRange   int
	MaxRange   int
}

// Generates hash Value between 0-10000
func generteHash(Id string) int {

	hasher := murmur3.New32WithSeed(seed)
	hasher.Write([]byte(Id))

	// generates int hash value in range of 0-10000
	hashval := int(hasher.Sum32() % slots)
	return hashval
}

// function to validate if total percentage is less than or equal to 100
func validatePercentageSplit(buckets []Bucket) bool {

	totalPercentage := 0.00

	for _, bucket := range buckets {

		totalPercentage = totalPercentage + bucket.Percentage
	}

	return totalPercentage <= 100

}

// Creates allocation mappings for bucket allocation
// returns allocation and err if any
func CreateAllocations(buckets []Bucket) (*Allocations, error) {

	if !validatePercentageSplit(buckets) {
		return nil, errors.New("total percentage should be <= 100")
	}

	mappings := make([]Allocation, len(buckets))
	currentmin := 0
	for index, bucket := range buckets {

		maxLimit := bucket.Percentage / 100 * 10000
		maxrange := currentmin + int(maxLimit)
		mappings[index] = Allocation{
			MinRange:   currentmin,
			MaxRange:   maxrange,
			Percentage: bucket.Percentage,
			Name:       bucket.Bucketname,
		}
		currentmin = maxrange + 1

	}
	return &Allocations{Mappings: mappings}, nil

}

// function to get bucket allocation for an id
// returns name of the bucket if allocated or else empty string
func (all *Allocations) GetBucketAllocation(Id string) string {

	hashval := generteHash(Id)
	result := ""
	for _, allocation := range all.Mappings {

		if hashval >= allocation.MinRange && hashval <= allocation.MaxRange {
			result = allocation.Name
			break
		}

	}
	return result

}

var ErrUnsupportedKeyType = errors.New("unsupported key type")

type Hasher interface {
	Hash(interface{}) (int, error)
}

// Default implementation of Hasher interface,
// which can hash key of string or int types
type DefaultHasher struct{}

func (dh *DefaultHasher) Hash(key interface{}) (int, error) {
	switch k := key.(type) {
	case string:
		h := murmur3.New32WithSeed(seed)
		_, err := h.Write([]byte(k))
		if err != nil {
			return -1, err
		}
		return int(h.Sum32()), nil
	case int:
		return k, nil
	default:
		return -1, ErrUnsupportedKeyType
	}
}

// AllAllocBktUsingHasher tries to allocate a bucket to the specified key using
func (a *Allocations) AllocBktUsingHasher(h Hasher, key interface{}) (string, error) {
	// use default hasher if no custom hasher was provided
	if h == nil {
		h = &DefaultHasher{}
	}

	// calculate the hash and bound it in available slots
	hash, err := h.Hash(key)
	if err != nil {
		return "", err
	}
	hash = hash % slots

	for _, alloc := range a.Mappings {
		if hash >= alloc.MinRange && hash <= alloc.MaxRange {
			return alloc.Name, nil
		}
	}

	return "", fmt.Errorf("unable to allocate a bucket to `%v`", key)
}
