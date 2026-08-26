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
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// Action is the log field value that marks banner entries.
const Action = "banner"

// Fetcher returns the current art; Fetch is the production one.
type Fetcher func(ctx context.Context) ([]string, error)

// Repeater logs the banner again on an interval, drawing the art fetched from
// the website when there is one and EmbeddedArt otherwise. It runs on its own
// timer: the telemetry ticker never fires with telemetry off and not until
// its first push succeeds, so nothing here depends on it.
type Repeater struct {
	logger   logrus.FieldLogger
	restURL  string
	interval time.Duration
	fetch    Fetcher
	art      atomic.Pointer[[]string]
}

// NewRepeater builds a repeater; a non-positive interval means
// DefaultInterval, and a nil fetcher means Fetch against ArtURL.
func NewRepeater(logger logrus.FieldLogger, restURL string, interval time.Duration, fetch Fetcher) *Repeater {
	if interval <= 0 {
		interval = DefaultInterval
	}
	if fetch == nil {
		client := NewClient()
		fetch = func(ctx context.Context) ([]string, error) { return Fetch(ctx, client, ArtURL) }
	}
	return &Repeater{logger: logger, restURL: restURL, interval: interval, fetch: fetch}
}

// Run fetches the art, then repeats the banner every interval until ctx is
// done. The startup banner was already logged by the caller, so the first
// emission is one interval in.
func (r *Repeater) Run(ctx context.Context) {
	r.refresh(ctx)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.refresh(ctx)
			r.emit()
		}
	}
}

func (r *Repeater) refresh(ctx context.Context) {
	fctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	art, err := r.fetch(fctx)
	if err != nil {
		// Debug, not error: a node without egress fails this forever.
		r.logger.WithField("action", Action).Debugf("banner art not fetched, keeping the last known art: %v", err)
		return
	}
	r.art.Store(&art)
}

// Art is what the next emission draws.
func (r *Repeater) Art() []string {
	if p := r.art.Load(); p != nil {
		return *p
	}
	return EmbeddedArt
}

func (r *Repeater) emit() {
	docsURL := enterrors.WithClusterID(LandingURL)
	r.logger.WithFields(logrus.Fields{
		"action":                Action,
		enterrors.DocsLinkField: docsURL,
	}).Info(Render(r.Art(), r.restURL, docsURL, "Running"))
}
