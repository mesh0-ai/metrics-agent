package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// Multi-event datagrams.
//
// A datagram normally carries ONE event. An emitter that produces many small
// events per request (Xano's exact flow evidence: ~200 for a request making
// 100 function calls, each ~1.2 KB of which ~340 B is the per-datagram
// `_token`) pays one send, one token and one listener/demuxer/batcher hop per
// event. A batch datagram carries one routing header and N events:
//
//	M0BATCH/1\n{"_project":"workspace-1","_token":"…","events":[{…},{…}]}
//
// The first line is the magic and the format version. It is not JSON on
// purpose: an agent that predates this format drops the datagram as one
// parse_error instead of forwarding `{"events":[…]}` as an event (which the
// gateway would refuse, failing every other event in its POST).
//
// `_project` and `_token` route and authorize every event, exactly as on a
// single datagram; both are optional. No other header member is accepted.
//
// Each event is one JSON object. A member's own `_token` is stripped and
// ignored (the header's authorizes it); a member `_project` must equal the
// header's, else that member is dropped as parse_error.
//
// An emitter must not send one unless the agent advertises it: on bind the
// agent writes `<listen path>.caps` naming the formats it accepts and the
// socket it is bound to (dev/inode), and removes it on exit. An emitter that
// checks the inode against the socket it connected to cannot be fooled by a
// file left behind by an agent that was replaced by an older one.
const (
	batchDatagramMagic   = "M0BATCH/"
	batchDatagramVersion = "1"
	batchFormatV1        = "m0batch/1"
)

var batchDatagramPrefix = []byte(batchDatagramMagic)

func isBatchDatagram(b []byte) bool {
	return bytes.HasPrefix(b, batchDatagramPrefix)
}

type batchParse int

const (
	batchParsed batchParse = iota
	batchUnsupported
	batchMalformed
)

// batchDatagram is a parsed batch: the header's routing fields (with the
// same bad* flags extractAndStripRouting reports) and the events, each its
// own copy of the bytes.
type batchDatagram struct {
	project    string
	token      string
	badProject bool
	badToken   bool
	events     [][]byte
}

// parseBatchDatagram splits a batch datagram. It accepts only the exact
// shape above: one version line, then one JSON object holding `events` (a
// non-empty array of at most MaxEventsPerBatch values) and optionally
// `_project`/`_token`, and nothing after it. Events are returned as the raw
// JSON the decoder read; whether each is an object is the batcher's check,
// as it is for a single datagram.
func parseBatchDatagram(b []byte) (batchDatagram, batchParse) {
	var bd batchDatagram
	if !isBatchDatagram(b) {
		return bd, batchMalformed
	}
	rest := b[len(batchDatagramPrefix):]
	nl := bytes.IndexByte(rest, '\n')
	if nl < 0 {
		return bd, batchMalformed
	}
	if string(rest[:nl]) != batchDatagramVersion {
		return bd, batchUnsupported
	}

	f := extractAndStripRouting(rest[nl+1:])
	if f.malformed {
		return bd, batchMalformed
	}
	bd.project, bd.token = f.project, f.token
	bd.badProject, bd.badToken = f.badProject, f.badToken

	dec := json.NewDecoder(bytes.NewReader(f.stripped))
	dec.DisallowUnknownFields()
	var payload struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := dec.Decode(&payload); err != nil {
		return bd, batchMalformed
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return bd, batchMalformed
	}
	if len(payload.Events) == 0 || len(payload.Events) > MaxEventsPerBatch {
		return bd, batchMalformed
	}
	bd.events = make([][]byte, len(payload.Events))
	for i, ev := range payload.Events {
		bd.events[i] = ev
	}
	return bd, batchParsed
}

// eventCount is how many events a queued datagram carries: one, or a
// batch's. Every drop of a queued datagram is accounted this many times.
func (dg rawDatagram) eventCount() uint64 {
	if dg.events != nil {
		return uint64(len(dg.events))
	}
	return 1
}

// ---- advertisement ---------------------------------------------------------

// agentCaps is the JSON of the capability file.
type agentCaps struct {
	Agent            string     `json:"agent"`
	Version          string     `json:"version"`
	Formats          []string   `json:"formats"`
	MaxDatagramBytes int        `json:"max_datagram_bytes"`
	MaxBatchEvents   int        `json:"max_batch_events"`
	Socket           capsSocket `json:"socket"`
}

// capsSocket names the socket the advertisement is for. An emitter compares
// it with stat() of the path it connects to and ignores the file on a
// mismatch, i.e. when the socket was re-bound by another agent.
type capsSocket struct {
	Dev uint64 `json:"dev"`
	Ino uint64 `json:"ino"`
}

func capsPath(listenPath string) string {
	return listenPath + ".caps"
}

// writeCapsFile advertises batch datagrams for the socket now bound at
// listenPath. Written to a temporary file and renamed, so an emitter never
// reads half of it, and world-readable, since the emitter commonly runs as
// another uid (the socket itself is 0666 for the same reason).
func writeCapsFile(listenPath string, maxDatagram int) error {
	fi, err := os.Stat(listenPath)
	if err != nil {
		return err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("socket identity unavailable on this platform")
	}
	body, err := json.Marshal(agentCaps{
		Agent:            "mesh0-metrics-agent",
		Version:          Version,
		Formats:          []string{batchFormatV1},
		MaxDatagramBytes: maxDatagram,
		MaxBatchEvents:   MaxEventsPerBatch,
		Socket:           capsSocket{Dev: uint64(st.Dev), Ino: uint64(st.Ino)},
	})
	if err != nil {
		return err
	}
	final := capsPath(listenPath)
	tmp, err := os.CreateTemp(filepath.Dir(final), filepath.Base(final)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, final); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

// removeCapsFile withdraws the advertisement. Missing is fine.
func removeCapsFile(listenPath string) error {
	err := os.Remove(capsPath(listenPath))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
