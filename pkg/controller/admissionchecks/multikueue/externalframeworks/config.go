/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package externalframeworks

import (
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
	k8serrors "k8s.io/apimachinery/pkg/util/errors"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta1"
)

var (
	// adapters holds the configured adapters.
	adapters []*Adapter
)

// Initialize loads the provided external framework configurations, validates them, and constructs adapters for each unique GroupVersionKind (GVK).
// It returns an aggregated error containing parse or duplicate-configuration errors when validation fails. On success, the package-level `adapters` slice
// is populated with an Adapter for each configured GVK.
func Initialize(configs []configapi.MultiKueueExternalFramework) error {
	adapters = nil // Reset on re-initialization
	configsMap := make(map[schema.GroupVersionKind]configapi.MultiKueueExternalFramework)
	var errs []error

	for _, config := range configs {
		gvk, err := parseGVK(config.Name)
		if err != nil {
			errs = append(errs, fmt.Errorf("invalid external framework configuration for %q: %w", config.Name, err))
			continue
		}

		if _, exists := configsMap[*gvk]; exists {
			errs = append(errs, fmt.Errorf("duplicate configuration for GVK %s", gvk))
			continue
		}

		configsMap[*gvk] = config
	}

	if len(errs) > 0 {
		return k8serrors.NewAggregate(errs)
	}

	for gvk := range configsMap {
		adapters = append(adapters, &Adapter{gvk: gvk})
	}
	return nil
}

// GetAllAdapters returns the current list of configured adapters.
// The returned slice reflects the adapters populated by Initialize.
func GetAllAdapters() []*Adapter {
	return adapters
}

// parseGVK parses name into a schema.GroupVersionKind.
// It returns an error if name is empty or if the string is not a valid GVK representation.
func parseGVK(name string) (*schema.GroupVersionKind, error) {
	if name == "" {
		return nil, errors.New("name is required")
	}

	gvk, _ := schema.ParseKindArg(name)
	if gvk == nil {
		return nil, fmt.Errorf("invalid GVK format '%s'", name)
	}
	return gvk, nil
}