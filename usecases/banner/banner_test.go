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
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func TestEmbeddedArtGlyphs(t *testing.T) {
	// Only █ and ▁: the glyphs that keep a uniform width in Kibana's font.
	// No backslash: Grafana's newline unescaping leaves \\ doubled.
	for i, line := range EmbeddedArt {
		assert.NotContains(t, line, `\`, "line %d", i)
		assert.Regexp(t, `^[ █▁]+$`, line, "line %d", i)
		assert.LessOrEqual(t, len([]rune(line)), MaxArtColumns, "line %d", i)
	}
	assert.LessOrEqual(t, len(EmbeddedArt), MaxArtLines)
}

func TestRender(t *testing.T) {
	got := Render([]string{"  ██", "  ▁▁"}, nil, "http://localhost:8080", LandingURL()+"?clusterid=abc", "Running")

	assert.True(t, strings.HasPrefix(got, "\n  ██\n  ▁▁\n\n"), "art first, then a blank line: %q", got)
	assert.Contains(t, got, "  ► Docs:    "+LandingURL()+"?clusterid=abc\n")
	assert.Contains(t, got, "  ► Cluster: http://localhost:8080/v1/meta\n")
	assert.True(t, strings.HasSuffix(got, "  ► Status:  Running\n"))
	assert.NotContains(t, got, "News", "no news line without a message")
}

func TestRenderNews(t *testing.T) {
	news := "Weaviate 1.39 is out: https://weaviate.io/blog/weaviate-1-39-release"
	got := Render(nil, []string{news}, "http://localhost:8080", LandingURL(), "Running")
	assert.True(t, strings.HasSuffix(got, "  ► Status:  Running\n  ► News:    "+news+"\n"), "got %q", got)

	// A second line starts in the column the first line's text starts in.
	got = Render(nil, []string{"first", "second"}, "http://localhost:8080", LandingURL(), "Running")
	indent := strings.Repeat(" ", len("  ► News:    ")-len("►")+1)
	assert.True(t, strings.HasSuffix(got, "  ► News:    first\n"+indent+"second\n"), "got %q", got)
}

func TestRenderDropsControlCharacters(t *testing.T) {
	got := Render(nil, []string{"new\nlevel=error msg=forged"}, "http://h:1\nlevel=error msg=forged\r\x1b[31m", LandingURL(), "up\ndown")

	assert.Contains(t, got, "► Cluster: http://h:1level=error msg=forged[31m/v1/meta\n")
	assert.Contains(t, got, "► Status:  updown\n")
	assert.Contains(t, got, "► News:    newlevel=error msg=forged\n")
	assert.NotContains(t, got, "\nlevel=error")
}

func TestSanitize(t *testing.T) {
	wide := strings.Repeat("█", MaxArtColumns+5)

	tests := []struct {
		name    string
		in      []string
		want    []string
		wantErr bool
	}{
		{name: "kept as is", in: []string{"  ██▁", "  ▁██"}, want: []string{"  ██▁", "  ▁██"}},
		{name: "control characters dropped", in: []string{"██\n\x1b[31m▁\r"}, want: []string{"██[31m▁"}},
		{name: "lines cut to the column limit", in: []string{wide}, want: []string{strings.Repeat("█", MaxArtColumns)}},
		{
			name: "line count capped",
			in:   []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"},
			want: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
		},
		{name: "trailing blank lines dropped", in: []string{"██", "", "   "}, want: []string{"██"}},
		{name: "nothing visible", in: []string{"", "\n", "\x00"}, wantErr: true},
		{name: "empty", in: nil, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Sanitize(tt.in, MaxArtLines)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFetch(t *testing.T) {
	serve := func(status int, contentType, body string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "application/json", r.Header.Get("Accept"))
			assert.True(t, strings.HasPrefix(r.Header.Get("User-Agent"), "weaviate/"))
			w.Header().Set("Content-Type", contentType)
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}))
	}

	tests := []struct {
		name        string
		status      int
		contentType string
		body        string
		want        *Content
		wantErr     string
	}{
		{
			name: "art", status: 200, contentType: "application/json; charset=utf-8",
			body: `{"schema_version":1,"art":["  ██","  ▁▁"],"_comment":"ignored"}`,
			want: &Content{Art: []string{"  ██", "  ▁▁"}},
		},
		{
			name: "art with news", status: 200, contentType: "application/json",
			body: `{"schema_version":1,"art":["  ██"],"message":["Weaviate 1.39 is out: https://weaviate.io/blog/weaviate-1-39-release","","more","a fourth line is cut"]}`,
			want: &Content{Art: []string{"  ██"}, Message: []string{"Weaviate 1.39 is out: https://weaviate.io/blog/weaviate-1-39-release", "", "more"}},
		},
		{
			name: "blank news is dropped", status: 200, contentType: "application/json",
			body: `{"schema_version":1,"art":["  ██"],"message":[""," ","\u0000"]}`,
			want: &Content{Art: []string{"  ██"}},
		},
		{
			name: "text/plain as raw file hosts serve it", status: 200, contentType: "text/plain; charset=utf-8",
			body: `{"schema_version":1,"art":["  ██"]}`, want: &Content{Art: []string{"  ██"}},
		},
		{name: "not found", status: 404, contentType: "application/json", body: `{}`, wantErr: "unexpected status 404"},
		{name: "html error page", status: 200, contentType: "text/html", body: `<html>`, wantErr: "unexpected content type"},
		{name: "not json", status: 200, contentType: "application/json", body: `nope`, wantErr: "decode"},
		{name: "unknown schema", status: 200, contentType: "application/json", body: `{"schema_version":2,"art":["x"]}`, wantErr: "unsupported schema_version 2"},
		{name: "no art", status: 200, contentType: "application/json", body: `{"schema_version":1,"art":[]}`, wantErr: "no visible lines"},
		{
			name: "oversized body", status: 200, contentType: "application/json",
			body: `{"schema_version":1,"art":["` + strings.Repeat("x", maxBodyBytes) + `"]}`, wantErr: "exceeds",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := serve(tt.status, tt.contentType, tt.body)
			defer srv.Close()

			got, err := Fetch(context.Background(), srv.Client(), srv.URL)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFetchHonoursContextTimeout(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-block }))
	defer func() { close(block); srv.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := Fetch(ctx, srv.Client(), srv.URL)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRepeater(t *testing.T) {
	t.Cleanup(func() { enterrors.SetClusterIDSource(nil) })
	const id = "0198c0de-dead-beef-8000-000000000001"
	var committed atomic.Bool
	clusterID := func() string {
		if committed.Load() {
			return id
		}
		return ""
	}
	enterrors.SetClusterIDSource(clusterID)

	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	fetched := &Content{Art: []string{"  ██ remote"}, Message: []string{"Weaviate 1.39 is out: https://weaviate.io/blog/weaviate-1-39-release"}}
	calls := 0
	fetch := func(context.Context) (*Content, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("offline")
		}
		return fetched, nil
	}

	r := NewRepeater(logger, clusterID, "http://localhost:8080", 5*time.Millisecond, fetch)
	assert.Equal(t, &Content{Art: EmbeddedArt}, r.Content(), "nothing fetched yet")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	// No id yet: nothing is logged, however long we wait.
	time.Sleep(30 * time.Millisecond)
	assert.Empty(t, hook.AllEntries(), "no banner before the cluster has an id")
	committed.Store(true)

	var banners []*logrus.Entry
	require.Eventually(t, func() bool {
		banners = nil
		for _, e := range hook.AllEntries() {
			if e.Level == logrus.InfoLevel && e.Data["action"] == Action {
				banners = append(banners, e)
			}
		}
		return len(banners) >= 2
	}, 2*time.Second, time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}

	// The failed first fetch is a debug entry, never an error.
	var failedFetch int
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, "banner art not fetched") {
			assert.Equal(t, logrus.DebugLevel, e.Level)
			failedFetch++
		}
	}
	assert.Equal(t, 1, failedFetch)

	last := banners[len(banners)-1]
	docsURL := LandingURL() + "?clusterid=0198c0de-dead-beef-8000-000000000001"
	assert.Equal(t, docsURL, last.Data[enterrors.DocsLinkField])
	assert.Contains(t, last.Message, "  ██ remote\n", "the fetched art replaces the embedded art")
	assert.Contains(t, last.Message, "► Docs:    "+docsURL)
	assert.Contains(t, last.Message, "► Status:  Running")
	assert.Contains(t, last.Message, "► News:    Weaviate 1.39 is out: https://weaviate.io/blog/weaviate-1-39-release\n")
	assert.Equal(t, fetched, r.Content())
}

func TestRepeaterStopsWhileWaitingForClusterID(t *testing.T) {
	logger, hook := test.NewNullLogger()
	r := NewRepeater(logger, func() string { return "" }, "http://localhost:8080", time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
	assert.Empty(t, hook.AllEntries())
}

func TestNewRepeaterDefaults(t *testing.T) {
	r := NewRepeater(logrus.New(), func() string { return "" }, "http://localhost:8080", 0, nil)
	assert.Equal(t, DefaultInterval, r.interval)
	assert.NotNil(t, r.fetch)
}
