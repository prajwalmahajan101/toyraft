#!/usr/bin/env bash
# scripts/check-no-time-now.sh
#
# Forbids time.Now() in core Raft packages. The only sanctioned call
# site is internal/clock/real.go (Real.Now), which exists precisely so
# the rest of the codebase can take a clock.Clock dependency instead of
# importing time directly. Test files (*_test.go) are exempt — tests
# need wall-clock for SC4-style budget assertions.
#
# Background: RESEARCH §Pitfall 3. Drift here causes determinism to
# collapse silently under -race -count=100. ADR-0006 (FakeClock) and
# ADR-0007 (Hub chaos) both depend on this rule.
#
# Targets:
#   pkg/raft               — core Raft state machine (lands in Phase 5+)
#   pkg/transport/inproc   — Hub + dispatcher; must take Clock from cfg
#   internal/raftest       — cluster harness; FakeClock-derived nanos
#                            for HistoryEvent (lands in plan 04-05)
#
# Allowlist:
#   internal/clock/real.go — Real.Now MUST call time.Now (wraps stdlib)
#
# Exit 0 on clean, 1 on hit.

set -euo pipefail

TARGETS=(
    pkg/raft
    pkg/transport/inproc
    internal/raftest
)

FAIL=0
for dir in "${TARGETS[@]}"; do
    if [ ! -d "$dir" ]; then
        # internal/raftest lands in plan 04-05; pkg/raft populates in
        # Phase 5+. Absence of a target tree is not a failure.
        continue
    fi
    if command -v rg >/dev/null 2>&1; then
        hits=$(rg -n --no-heading 'time\.Now\(\)' "$dir" \
            --glob '!*_test.go' || true)
    else
        hits=$(grep -rn 'time\.Now()' "$dir" \
            --include='*.go' --exclude='*_test.go' || true)
    fi
    if [ -n "$hits" ]; then
        echo "FAIL: time.Now() found in $dir (non-test code):"
        echo "$hits"
        FAIL=1
    fi
done

# Belt-and-braces: confirm the allowlisted sanctioned site still uses
# stdlib time.Now — if someone refactors it away, the rest of the lint
# becomes meaningless (everyone would just allowlist their own file).
if [ -f internal/clock/real.go ]; then
    if ! grep -q 'time\.Now()' internal/clock/real.go; then
        echo "FAIL: internal/clock/real.go no longer calls time.Now();"
        echo "      the wall-clock entry point has moved — update this script."
        FAIL=1
    fi
fi

if [ "$FAIL" -ne 0 ]; then
    echo ""
    echo "Fix: receive a clock.Clock and call clk.Now() instead. The"
    echo "only sanctioned time.Now() call site is internal/clock/real.go."
    exit 1
fi
echo "OK: no time.Now() leaks in core packages"
