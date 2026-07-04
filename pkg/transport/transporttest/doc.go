// Package transporttest holds the SHARED behavioural conformance suite for
// raft.Transport implementations.
//
// RunConformance is the single exported entry point (SC1): both the in-process
// Hub transport (pkg/transport/inproc) and the HTTP transport (pkg/transport/http,
// wave 2) call it against their own connected-pair factory, proving they satisfy
// one common contract. This is the LLD top-level "conformance suite a third-party
// Transport implementor can run" — it depends only on the frozen raft.Transport
// surface, never on any concrete implementation, so a downstream implementor can
// import it to certify their own transport.
package transporttest
