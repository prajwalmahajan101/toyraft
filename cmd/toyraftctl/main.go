// Command toyraftctl is the reference ToyRaft CLI (DEMO-05): a thin http.Client
// with subcommands get/set/del/status/members targeting a node's CLIENT api via
// -addr. It follows the 307 leader redirect (WIRE §5.1), so a command aimed at
// any node reaches the leader.
//
// The subtle correctness point (10-RESEARCH Open-Q 3 / Pitfall 6): Go's default
// http.Client follows a 307 AND preserves method+body only when Request.GetBody
// is set. http.NewRequest sets GetBody automatically when the body is a
// *bytes.Reader/*strings.Reader — so PUT bodies are built via bytes.NewReader,
// letting the redirected PUT re-send the value to the leader intact.
//
// /status role is the LOWERCASE string contract locked in 10-02
// ("follower"/"candidate"/"leader"), decoded into a Go string field — never an
// int. STDLIB ONLY: net/http, bytes, encoding/json, flag, fmt, io, os, time.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// errNotFound is returned by doGet when the key is absent (404), so main can map
// it to the "key not found" stderr message + non-zero exit without conflating it
// with a transport error.
var errNotFound = errors.New("key not found")

// statusResp mirrors the daemon's GET /status wire shape (cmd/toyraftd/kvapi.go
// statusResp). Role is the LOWERCASE STRING contract from 10-02 — decoded into a
// string, NEVER an int.
type statusResp struct {
	Role        string            `json:"role"`
	Term        uint64            `json:"term"`
	CommitIndex uint64            `json:"commit_index"`
	ApplyIndex  uint64            `json:"apply_index"`
	LeaderHint  string            `json:"leader_hint"`
	Members     map[string]uint64 `json:"members"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9001", "target node's CLIENT api host:port (the CLI follows the 307 to the leader)")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(),
			"toyraftctl — reference ToyRaft CLI (client of a node's CLIENT api)\n\n"+
				"Usage: toyraftctl [-addr host:port] <command> [args]\n\n"+
				"Commands:\n"+
				"  set <key> <value>   PUT /kv/<key> (body=value); follows 307 to the leader\n"+
				"  get <key>           GET /kv/<key>; prints value, exit 1 if not found\n"+
				"  del <key>           DELETE /kv/<key>; follows 307 to the leader\n"+
				"  status              GET /status; pretty-print role/term/commit/apply\n"+
				"  members             GET /status; pretty-print the peer set\n\n"+
				"Flags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	base := "http://" + *addr
	client := &http.Client{Timeout: 5 * time.Second}

	if err := dispatch(client, base, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "toyraftctl:", err)
		os.Exit(1)
	}
}

// usageError signals an unknown subcommand or missing args; main-level dispatch
// maps it to exit code 2 (distinct from a runtime failure's exit 1).
type usageError struct{ msg string }

func (e *usageError) Error() string { return e.msg }

// dispatch routes flag.Args() to the subcommand handlers. It exits the process
// directly for get (which must print the value to stdout and control its own
// exit code) and for usage errors (exit 2); all other errors are returned to
// main for the exit-1 path.
func dispatch(client *http.Client, base string, args []string) error {
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "set":
		if len(rest) != 2 {
			return usage("set requires <key> <value>")
		}
		return doSet(client, base, rest[0], rest[1])
	case "get":
		if len(rest) != 1 {
			return usage("get requires <key>")
		}
		val, err := doGet(client, base, rest[0])
		if errors.Is(err, errNotFound) {
			fmt.Fprintln(os.Stderr, "key not found")
			os.Exit(1)
		}
		if err != nil {
			return err
		}
		fmt.Print(val)
		return nil
	case "del":
		if len(rest) != 1 {
			return usage("del requires <key>")
		}
		return doDel(client, base, rest[0])
	case "status":
		if len(rest) != 0 {
			return usage("status takes no args")
		}
		s, err := doStatus(client, base)
		if err != nil {
			return err
		}
		printStatus(s)
		return nil
	case "members":
		if len(rest) != 0 {
			return usage("members takes no args")
		}
		s, err := doStatus(client, base)
		if err != nil {
			return err
		}
		printMembers(s)
		return nil
	default:
		return usage(fmt.Sprintf("unknown command %q", cmd))
	}
}

// usage prints usage to stderr and exits 2 — an invocation error, distinct from
// a runtime failure. It never returns (the signature keeps callers as one-liners).
func usage(msg string) error {
	fmt.Fprintln(os.Stderr, "toyraftctl:", msg)
	flag.Usage()
	os.Exit(2)
	return &usageError{msg} // unreachable; satisfies the signature
}

// doSet issues PUT /kv/<key> with value as the body. The body is a
// bytes.NewReader so http.NewRequest sets Request.GetBody — this is what lets the
// default http.Client re-send the PUT with its body when a follower answers 307
// (10-RESEARCH Pitfall 6). A non-2xx surfaces the server envelope as an error.
func doSet(client *http.Client, base, key, value string) error {
	req, err := http.NewRequest(http.MethodPut, base+"/kv/"+key, bytes.NewReader([]byte(value)))
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return expect2xx(resp)
}

// doGet issues GET /kv/<key>. 200 returns the value body; 404 returns
// errNotFound; any other status surfaces the server envelope.
func doGet(client *http.Client, base, key string) (string, error) {
	resp, err := client.Get(base + "/kv/" + key)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}
		return string(body), nil
	case http.StatusNotFound:
		return "", errNotFound
	default:
		return "", envelopeError(resp)
	}
}

// doDel issues DELETE /kv/<key>. The body is nil (a delete carries no value), so
// there is nothing to replay across a 307 — the method is preserved by 307 alone.
func doDel(client *http.Client, base, key string) error {
	req, err := http.NewRequest(http.MethodDelete, base+"/kv/"+key, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return expect2xx(resp)
}

// doStatus issues GET /status and decodes the JSON into statusResp. role decodes
// as a lowercase STRING per the 10-02 contract (never an int).
func doStatus(client *http.Client, base string) (statusResp, error) {
	var s statusResp
	resp, err := client.Get(base + "/status")
	if err != nil {
		return s, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return s, envelopeError(resp)
	}
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return s, fmt.Errorf("decode /status: %w", err)
	}
	return s, nil
}

// printStatus pretty-prints the node's role/term/commit/apply/leader to stdout.
func printStatus(s statusResp) {
	fmt.Printf("role:         %s\n", s.Role)
	fmt.Printf("term:         %d\n", s.Term)
	fmt.Printf("commit_index: %d\n", s.CommitIndex)
	fmt.Printf("apply_index:  %d\n", s.ApplyIndex)
	fmt.Printf("leader_hint:  %s\n", s.LeaderHint)
}

// printMembers pretty-prints the peer set carried in /status (RESEARCH Open-Q 4:
// membership rides on /status, so members needs no new raft API). The map is
// range-printed; ordering is not asserted (the demo readout is diagnostic).
func printMembers(s statusResp) {
	if len(s.Members) == 0 {
		fmt.Println("(no members reported)")
		return
	}
	for id, matchIndex := range s.Members {
		fmt.Printf("%s\tmatch_index=%d\n", id, matchIndex)
	}
}

// expect2xx returns nil for a 2xx response and otherwise the server error
// envelope — no silent fallback on a non-2xx write.
func expect2xx(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return envelopeError(resp)
}

// envelopeError reads the response body and wraps it (the server's JSON error
// envelope, e.g. {"error":"proposal_dropped"}) into an error carrying the status.
func envelopeError(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	msg := string(bytes.TrimSpace(body))
	if msg == "" {
		return fmt.Errorf("server returned %s", resp.Status)
	}
	return fmt.Errorf("server returned %s: %s", resp.Status, msg)
}
