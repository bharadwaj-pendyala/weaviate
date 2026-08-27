//	_       _
//
// __      _____  __ ___   ___  __ _| |_ ___
//
//	\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//	 \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//	  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//	 Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//	 CONTACT: hello@weaviate.io
package batch

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

func TestErrorMessage(t *testing.T) {
	tests := []struct {
		name      string
		principal *models.Principal
		err       error
		want      string
	}{
		{
			name: "undocumented error is passed through",
			err:  fmt.Errorf("invalid object: something else"),
			want: "invalid object: something else",
		},
		{
			name: "documented error gets the page appended",
			err:  fmt.Errorf("put object: cannot init shard: %w", enterrors.ErrNotEnoughMappings),
			want: "put object: cannot init shard: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)",
		},
		{
			name:      "own namespace is stripped and the link kept",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			err:       fmt.Errorf("put object: customer1:Articles: cannot init shard: %w", enterrors.ErrNotEnoughMappings),
			want:      "put object: Articles: cannot init shard: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)",
		},
		{
			name:      "namespace named after the link's scheme leaves the link intact",
			principal: &models.Principal{Username: "u", Namespace: "https"},
			err:       fmt.Errorf("put object: https:Articles: cannot init shard: %w", enterrors.ErrNotEnoughMappings),
			want:      "put object: Articles: cannot init shard: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, errorMessage(tt.principal, tt.err))
		})
	}
}
