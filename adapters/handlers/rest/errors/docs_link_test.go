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

package errors

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func TestErrPayloadFromSingleErrDocsLink(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "undocumented error is passed through",
			err:  fmt.Errorf("updating db: something else"),
			want: "updating db: something else",
		},
		{
			name: "documented error gets the page appended",
			err:  fmt.Errorf("updating db: TYPE_UPDATE_TENANT: memory pressure: cannot init shard: %w", enterrors.ErrNotEnoughMappings),
			want: "updating db: TYPE_UPDATE_TENANT: memory pressure: cannot init shard: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := ErrPayloadFromSingleErr(nil, tt.err)
			require.Len(t, payload.Error, 1)
			assert.Equal(t, tt.want, payload.Error[0].Message)
		})
	}
}
