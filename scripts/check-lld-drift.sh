#!/usr/bin/env bash
# scripts/check-lld-drift.sh
#
# SC6 drift check (Phase 2 / .planning/phases/02-foundations/02-RESEARCH.md §8):
# verifies that `go doc -all ./pkg/raft` matches docs/lld-go-doc-golden.txt
# byte-for-byte. Any divergence means either (a) the implementation drifted
# from docs/LLD.md (FAIL — fix the code), or (b) docs/LLD.md genuinely
# changed and the golden needs regeneration (UPDATE — `make lld-drift-update`
# + commit both the LLD change and the new golden in the same PR; reviewer
# enforces that the semantic intent matches).
#
# Exit 0 on match, 1 on drift.

set -euo pipefail

GOLDEN=docs/lld-go-doc-golden.txt
CURRENT=$(mktemp)
trap 'rm -f "$CURRENT"' EXIT

go doc -all ./pkg/raft > "$CURRENT"

if ! diff -u "$GOLDEN" "$CURRENT"; then
    echo "" >&2
    echo "*** LLD drift detected ***" >&2
    echo "The go doc -all ./pkg/raft output differs from $GOLDEN." >&2
    echo "" >&2
    echo "If this drift is intentional (you updated docs/LLD.md in this PR):" >&2
    echo "    make lld-drift-update    # regenerates $GOLDEN" >&2
    echo "    git add $GOLDEN docs/LLD.md  # commit BOTH in the same change" >&2
    echo "" >&2
    echo "Otherwise: revert the pkg/raft change that caused the drift." >&2
    exit 1
fi

echo "LLD drift check: OK (no drift)"
