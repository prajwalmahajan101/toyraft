// Package http is the node-to-node Raft transport over HTTP/JSON.
//
// It marshals and unmarshals pkg/raft.Message values against the byte-exact
// wire contract frozen in docs/WIRE.md (v1). Every peer RPC is an independent
// fire-and-forget POST /raft/message carrying a JSON body (WIRE §1); there is
// no synchronous response envelope for the message itself — a 204 acknowledges
// that Node.Step accepted it, and any Raft reply arrives as a separate inbound
// POST from the peer (TRAN-02).
//
// The JSON projection lives in a dedicated wireMessage DTO (wire.go) so the
// frozen pkg/raft.Message stays tag-free (RESEARCH §"where JSON lives",
// ADR-0015). The decoder tolerates unknown JSON fields (WIRE §6.1 forward
// compatibility) and NEVER uses DisallowUnknownFields.
//
// This package uses only net/http and the standard library; it pulls in no
// external HTTP framework. All timing is routed through internal/clock.Clock —
// there is no direct time.Now() anywhere in the transport (see config.go).
//
// See docs/WIRE.md for the authoritative frame and error-sentinel tables and
// docs/adr/0015-http-transport-config-address-book.md for the Config surface.
package http
