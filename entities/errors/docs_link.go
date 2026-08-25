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
	stderrors "errors"
	"fmt"

	"github.com/sirupsen/logrus"
)

const (
	// DocsLinkField holds the link in a log entry's structured data instead of
	// its message: operators fingerprint and alert on message text, so a URL
	// appended there breaks their rules.
	DocsLinkField = "docs_url"

	// docsLinkBase resolves an id through a redirect kept in the docs repo, so
	// a docs restructure cannot rot a URL already compiled into a release.
	docsLinkBase = "https://docs.weaviate.io/e/"
)

// DocsID names a documented log message. The docs repo redirects /e/<id> to
// the page for it, so an id is a contract with that repo and never changes.
type DocsID string

const (
	DocsIDNotEnoughMappings DocsID = "core-mem001"
)

// DocsLink is the URL of the page that documents id.
func DocsLink(id DocsID) string {
	return docsLinkBase + string(id)
}

// DocsLinkFieldsFor returns the log fields pointing at the page for id, for a
// log site that knows which documented condition it reports.
func DocsLinkFieldsFor(id DocsID) logrus.Fields {
	return logrus.Fields{DocsLinkField: DocsLink(id)}
}

// Documented reports the id of the page documenting err, if it has one.
func Documented(err error) (DocsID, bool) {
	if stderrors.Is(err, ErrNotEnoughMappings) {
		return DocsIDNotEnoughMappings, true
	}

	return "", false
}

// DocsLinkFields returns the log fields pointing at the page that explains err,
// and nil for an error with no documented page. logrus ignores nil fields, so a
// log site reporting errors of mixed origin can add them unconditionally.
func DocsLinkFields(err error) logrus.Fields {
	if id, ok := Documented(err); ok {
		return DocsLinkFieldsFor(id)
	}

	return nil
}

// MessageWithDocsLink is err's message with the documenting page appended, for
// API callers who cannot see the docs_url log field. An undocumented error's
// message comes back unchanged. The message is formatted with %v, as the API
// error payloads always were, so nil reads "<nil>" and a broken Error method
// cannot fail a request.
func MessageWithDocsLink(err error) string {
	msg := fmt.Sprintf("%v", err)
	if id, ok := Documented(err); ok {
		return msg + " (see " + DocsLink(id) + ")"
	}

	return msg
}
