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

package banner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/weaviate/weaviate/usecases/build"
)

const (
	fetchTimeout = 5 * time.Second
	maxBodyBytes = 64 << 10
)

// artDocument is the shape of ArtURL. Unknown fields are ignored; an unknown
// schema_version is refused so a newer format is never half-read.
type artDocument struct {
	SchemaVersion int      `json:"schema_version"`
	Art           []string `json:"art"`
}

// Fetch downloads and sanitizes the banner art. Every failure is returned
// rather than logged, so the caller can keep it at debug level: an air-gapped
// node fails this on every repeat and must not see an error for it.
func Fetch(ctx context.Context, client *http.Client, url string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "weaviate/"+build.Version)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	// The check keeps an HTML error page out; text/plain is accepted because
	// raw file hosts serve JSON that way, and the decode below is the real test.
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") && !strings.HasPrefix(ct, "text/plain") {
		return nil, fmt.Errorf("unexpected content type %q", ct)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxBodyBytes {
		return nil, fmt.Errorf("body exceeds %d bytes", maxBodyBytes)
	}

	var doc artDocument
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if doc.SchemaVersion != 1 {
		return nil, fmt.Errorf("unsupported schema_version %d", doc.SchemaVersion)
	}
	return Sanitize(doc.Art)
}

// NewClient is the HTTP client the repeater fetches with: a short timeout, so
// a slow or black-holed connection can never hold a goroutine for long.
func NewClient() *http.Client {
	return &http.Client{Timeout: fetchTimeout}
}

// Sanitize keeps remote art within what a log line can carry: control
// characters are dropped, lines are cut to MaxArtColumns, at most MaxArtLines
// are kept, and art with no visible content is refused.
func Sanitize(lines []string) ([]string, error) {
	out := make([]string, 0, MaxArtLines)
	for _, line := range lines {
		if len(out) == MaxArtLines {
			break
		}
		clean := []rune(printable(line))
		if len(clean) > MaxArtColumns {
			clean = clean[:MaxArtColumns]
		}
		out = append(out, string(clean))
	}
	for len(out) > 0 && strings.TrimSpace(out[len(out)-1]) == "" {
		out = out[:len(out)-1]
	}
	if len(out) == 0 {
		return nil, errors.New("art has no visible lines")
	}
	return out, nil
}
