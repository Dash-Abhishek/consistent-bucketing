package gbucket

import (
	"testing"
)

func TestAllocateUsingHasherErrors(t *testing.T) {
	type testCase struct {
		key         interface{}
		expectedErr error
	}
	testCases := []testCase{
		{
			key:         10,
			expectedErr: nil,
		},
		{
			key:         "testStrType",
			expectedErr: nil,
		},
		{
			// testcase of a key which is not supported by default hasher
			key:         map[string]bool{"a": true},
			expectedErr: ErrUnsupportedKeyType,
		},
	}

	bkts := []Bucket{
		{Bucketname: "A", Percentage: 50},
		{Bucketname: "B", Percentage: 50},
	}
	a, _ := CreateAllocations(bkts)

	for idx, tc := range testCases {
		_, err := a.AllocBktUsingHasher(nil, tc.key)
		if err != tc.expectedErr {
			t.Errorf("case-%d fn: AllocateUsingHasher, err expected: %v found: %v", idx, tc.expectedErr, err)
		}
	}
}
