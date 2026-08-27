//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package batch

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

func TestErrorMessage(t *testing.T) {
	ns := &models.Principal{Username: "u", Namespace: "https"}
	documented := fmt.Errorf("put object: https:Articles: cannot init shard: %w", enterrors.ErrNotEnoughMappings)

	// The namespace is stripped before the link is appended, so a namespace
	// named after the link's scheme leaves the link intact.
	assert.Equal(t,
		"put object: Articles: cannot init shard: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)",
		errorMessage(ns, documented))
	assert.Equal(t, "invalid object: something else", errorMessage(ns, fmt.Errorf("invalid object: something else")))
	assert.Equal(t, "<nil>", errorMessage(nil, nil), "a nil error renders as the payloads always did")
}
