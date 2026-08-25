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

package rest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tailorincgraphql "github.com/tailor-platform/graphql"
	"github.com/tailor-platform/graphql/gqlerrors"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func TestAddDocsLinks(t *testing.T) {
	documented := fmt.Errorf("resolve Get: memory pressure: cannot load shard: %w", enterrors.ErrNotEnoughMappings)
	result := &tailorincgraphql.Result{Errors: []gqlerrors.FormattedError{
		// how the executor reports a resolver error: the returned error is
		// formatted, then located (wrapped in *gqlerrors.Error), then formatted again
		gqlerrors.FormatError(tailorincgraphql.NewLocatedErrorWithPath(gqlerrors.FormatError(documented), nil, nil)),
		gqlerrors.FormatError(documented),
		gqlerrors.FormatError(fmt.Errorf("resolve Get: something else")),
	}}

	addDocsLinks(result)

	want := "resolve Get: memory pressure: cannot load shard: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)"
	assert.Equal(t, want, result.Errors[0].Message)
	assert.Equal(t, want, result.Errors[1].Message)
	assert.Equal(t, "resolve Get: something else", result.Errors[2].Message)

	assert.NotPanics(t, func() { addDocsLinks(nil) })
	assert.NotPanics(t, func() { addDocsLinks(&tailorincgraphql.Result{}) })
}

// Runs the real executor so the nesting of library error types is whatever
// the library does today, not what this test assumes.
func TestAddDocsLinksThroughExecutor(t *testing.T) {
	documented := fmt.Errorf("explorer: list class: search: %w", enterrors.ErrNotEnoughMappings)
	schema, err := tailorincgraphql.NewSchema(tailorincgraphql.SchemaConfig{
		Query: tailorincgraphql.NewObject(tailorincgraphql.ObjectConfig{
			Name: "Query",
			Fields: tailorincgraphql.Fields{
				"Get": &tailorincgraphql.Field{
					Type: tailorincgraphql.String,
					Resolve: func(tailorincgraphql.ResolveParams) (interface{}, error) {
						// as the Get resolver returns it
						return nil, enterrors.NewErrGraphQLUser(documented, "Get", "Demo")
					},
				},
			},
		}),
	})
	require.NoError(t, err)

	result := tailorincgraphql.Do(tailorincgraphql.Params{Schema: schema, RequestString: "{ Get }"})
	require.Len(t, result.Errors, 1)

	addDocsLinks(result)

	assert.Equal(t, "explorer: list class: search: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)", result.Errors[0].Message)
}
