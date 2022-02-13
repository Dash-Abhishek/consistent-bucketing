package main

import (
	"fmt"

	"github.com/Dash-Abhishek/gbucket"
	"github.com/google/uuid"
)

func main() {

	bucketList := make([]gbucket.Bucket, 0)

	bucketList = append(bucketList, gbucket.Bucket{Percentage: 50, Bucketname: "A"})
	bucketList = append(bucketList, gbucket.Bucket{Percentage: 50, Bucketname: "B"})

	allocations, err := gbucket.CreateAllocations(&bucketList)
	if err == nil {

		fmt.Println(allocations)
		Emptycount := 0
		Acount := 0
		Bcount := 0
		for i := 0; i < 500; i++ {
			result := allocations.GetBucketAllocation(uuid.NewString())
			if result == "" {
				Emptycount++

			}
			if result == "A" {
				Acount++
			}
			if result == "B" {
				Bcount++
			}

		}
		fmt.Println("count", Emptycount, Acount, Bcount)

	} else {
		fmt.Println(err)
	}

}
