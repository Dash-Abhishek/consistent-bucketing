package gbucket

import (
	"errors"

	"github.com/spaolacci/murmur3"
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

	hasher := murmur3.New32WithSeed(10000)
	hasher.Write([]byte(Id))

	// generates int hash value in range of 0-10000
	hashval := int(hasher.Sum32() % 10000)
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
