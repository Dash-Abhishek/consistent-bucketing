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
func validatePercentageSplit(buckets *[]Bucket) bool {

	totalPercentage := 0.00

	for _, bucket := range *buckets {

		totalPercentage = totalPercentage + bucket.Percentage
	}

	return totalPercentage <= 100

}

// 5% of 100 is 5/100 * 100 = 5
// 5% of 1000 is 5/100 * 1000 = 50
// 50% of 1000 is 50/100 * 1000 = 500
// 25.5 of 1000 is 25.5/100 * 1000 = 255
// 25.55 of 1000 is 25.55/100 * 1000 = 255.5
// 25.55 of 10000 id 25.55/100 * 10000 = 2555
// function to create allocation mappings for bucket allocation
// returns pointer to allocation and err if any
func CreateAllocations(buckets *[]Bucket) (*Allocations, error) {

	if !validatePercentageSplit(buckets) {
		return nil, errors.New("total percentage should be <= 100")
	}

	mappings := make([]Allocation, len(*buckets))
	currentmin := 0
	for index, bucket := range *buckets {

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
