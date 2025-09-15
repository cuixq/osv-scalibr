// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package python

import (
	"reflect"
	"testing"
)

func TestTokenizeRequirement(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected []VersionConstraint
	}{
		{
			name:     "Empty string",
			input:    "",
			expected: []VersionConstraint{},
		},
		{
			name:  "Single constraint",
			input: "==1.2.3",
			expected: []VersionConstraint{
				{operator: "==", version: "1.2.3"},
			},
		},
		{
			name:  "Multiple constraints",
			input: ">=1.0,<2.0",
			expected: []VersionConstraint{
				{operator: ">=", version: "1.0"},
				{operator: "<", version: "2.0"},
			},
		},
		{
			name:  "Constraints with spaces",
			input: ">= 1.0, < 2.0",
			expected: []VersionConstraint{
				{operator: ">=", version: "1.0"},
				{operator: "<", version: "2.0"},
			},
		},
		{
			name:  "Compatible release operator",
			input: "~=2.2",
			expected: []VersionConstraint{
				{operator: "~=", version: "2.2"},
			},
		},
		{
			name:  "Not equal operator",
			input: "!=3.14",
			expected: []VersionConstraint{
				{operator: "!=", version: "3.14"},
			},
		},
		{
			name:  "Less than or equal operator",
			input: "<=5.0",
			expected: []VersionConstraint{
				{operator: "<=", version: "5.0"},
			},
		},
		{
			name:  "Greater than operator",
			input: ">0.1",
			expected: []VersionConstraint{
				{operator: ">", version: "0.1"},
			},
		},
		{
			name:  "Complex combination with mixed spacing",
			input: ">=1.2.3, < 2.0, != 1.5",
			expected: []VersionConstraint{
				{operator: ">=", version: "1.2.3"},
				{operator: "<", version: "2.0"},
				{operator: "!=", version: "1.5"},
			},
		},
		{
			name:  "No version specified",
			input: "==",
			expected: []VersionConstraint{
				{operator: "==", version: ""},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := tokenizeRequirement(tc.input)
			if !reflect.DeepEqual(actual, tc.expected) {
				t.Errorf("tokenizeRequirement(%q) = %v, want %v", tc.input, actual, tc.expected)
			}
		})
	}
}
