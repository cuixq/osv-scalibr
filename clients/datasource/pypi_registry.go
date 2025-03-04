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

package datasource

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"time"
)

const PyPIJSON = "https://pypi.org/pypi/requests/json"

const PyPISimple = "https://pypi.org/simple/requests"

// PyPIRegistryAPIClient defines a client to fetch metadata from a Maven registry.
type PyPIRegistryAPIClient struct {
	// Cache fields
	mu             *sync.Mutex
	cacheTimestamp *time.Time // If set, this means we loaded from a cache
	responses      *RequestCache[string, response]
}

// NewPyPIRegistryAPIClient returns a new PyPIRegistryAPIClient.
func NewPyPIRegistryAPIClient() (*PyPIRegistryAPIClient, error) {
	return &PyPIRegistryAPIClient{
		mu:        &sync.Mutex{},
		responses: NewRequestCache[string, response](),
	}, nil
}

func (m *PyPIRegistryAPIClient) get(ctx context.Context, url string, dst any) error {
	resp, err := m.responses.Get(url, func() (response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return response{}, err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return response{}, fmt.Errorf("%w: Maven registry query failed: %w", errAPIFailed, err)
		}
		defer resp.Body.Close()

		if !slices.Contains([]int{http.StatusOK, http.StatusNotFound, http.StatusUnauthorized}, resp.StatusCode) {
			// Only cache responses with Status OK, NotFound, or Unauthorized
			return response{}, fmt.Errorf("%w: Maven registry query status: %d", errAPIFailed, resp.StatusCode)
		}

		if b, err := io.ReadAll(resp.Body); err == nil {
			return response{StatusCode: resp.StatusCode, Body: b}, nil
		}

		return response{}, fmt.Errorf("failed to read body: %w", err)
	})
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: Maven registry query status: %d", errAPIFailed, resp.StatusCode)
	}

	return nil
}
