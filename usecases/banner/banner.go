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

// Package banner renders the startup banner and repeats it while the node runs.
package banner

import (
	"fmt"
	"strings"
	"time"

	"github.com/weaviate/weaviate/usecases/build"
)

const (
	// LandingURL is printed on every start and compiled into every release, so
	// the docs site keeps this path stable the way it keeps /e/<id> ids stable.
	LandingURL = "https://docs.weaviate.io/improve-your-cluster"

	// ArtURL serves the art the repeat banner draws. The file lives in the
	// website repository under static/banner/; a new shape gets a new file.
	ArtURL = "https://weaviate.io/banner/v1.json"

	// DefaultInterval is how often the banner is repeated after startup.
	DefaultInterval = 24 * time.Hour

	// MaxArtLines and MaxArtColumns bound what a log line and a terminal carry.
	MaxArtLines   = 10
	MaxArtColumns = 100
)

// EmbeddedArt is drawn at startup and whenever the art cannot be fetched. It
// uses only █ and ▁: the two Block Elements glyphs that keep a uniform width
// in Kibana's proportional font, and no backslash for Grafana's newline
// unescaping to double.
var EmbeddedArt = []string{
	"  ██▁▁▁▁▁██▁███████▁▁█████▁▁██▁▁▁▁██▁██▁▁█████▁▁████████▁███████",
	"  ██▁▁▁▁▁██▁██▁▁▁▁▁▁██▁▁▁██▁██▁▁▁▁██▁██▁██▁▁▁██▁▁▁▁██▁▁▁▁██▁▁▁▁▁",
	"  ██▁▁█▁▁██▁█████▁▁▁███████▁██▁▁▁▁██▁██▁███████▁▁▁▁██▁▁▁▁█████▁▁",
	"  ██▁███▁██▁██▁▁▁▁▁▁██▁▁▁██▁▁██▁▁██▁▁██▁██▁▁▁██▁▁▁▁██▁▁▁▁██▁▁▁▁▁",
	"  ▁███▁███▁▁███████▁██▁▁▁██▁▁▁████▁▁▁██▁██▁▁▁██▁▁▁▁██▁▁▁▁███████",
}

// Render builds the banner message: the art, then the version, the docs
// link, this node's /v1/meta URL and a status. Newlines stay in the message;
// the JSON formatter escapes them and log viewers render them back.
func Render(art []string, restURL, docsURL, status string) string {
	var b strings.Builder
	b.WriteString("\n")
	for _, line := range art {
		b.WriteString(line)
		b.WriteString("\n")
	}
	fmt.Fprintf(&b, "\n  ► Version: %s\n", build.Version)
	fmt.Fprintf(&b, "  ► Docs:    %s\n", printable(docsURL))
	fmt.Fprintf(&b, "  ► Cluster: %s/v1/meta\n", printable(restURL))
	fmt.Fprintf(&b, "  ► Status:  %s\n", printable(status))
	return b.String()
}

// printable drops control characters: the text formatter writes the banner
// verbatim, so a stray newline in a flag value would forge a log line.
func printable(s string) string {
	return strings.Map(func(r rune) rune {
		if r < ' ' || r == 0x7f || r == ' ' || r == ' ' {
			return -1
		}
		return r
	}, s)
}
