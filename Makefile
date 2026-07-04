# ToyRaft Makefile
# More targets land in later phases (build, test, demo) — see ROADMAP.md.

# N is the demo cluster size. It MUST be odd — raft.Config.Peers Validate
# hard-errors on even N (a clean majority needs an odd member count). The demo
# target guards this BEFORE launching anything.
N ?= 3

.PHONY: hooks lld-drift lld-drift-update check-no-time-now verify build-demo demo release-check release-snapshot

# build-demo compiles both reference binaries into bin/ (gitignored). The demo
# target depends on it so `make demo` is one command from a clean tree.
build-demo:
	go build -o bin/toyraftd ./cmd/toyraftd
	go build -o bin/toyraftctl ./cmd/toyraftctl

# demo boots an N-node cluster and runs the end-to-end smoke test (SC1/SC5/SC6).
# Client ports 9001..900N, peer ports 7001..700N. `make demo N=5` scales up;
# even N is rejected fast, before any process starts.
demo: build-demo
	@[ $$(( $(N) % 2 )) -eq 1 ] || { echo "N must be odd (got $(N))"; exit 1; }
	N=$(N) bash scripts/smoke.sh

hooks:
	@chmod +x .githooks/*
	git config core.hooksPath .githooks
	@echo "Hooks installed (pre-commit + commit-msg)"

# release-check validates .goreleaser.yml against the v2 schema (SC8 config
# gate — fails on any deprecated key such as the removed archives.replacements).
release-check:
	goreleaser check

# release-snapshot builds the release artifacts locally WITHOUT publishing:
# both binaries (toyraftd + toyraftctl) for {linux,macOS} x {amd64,arm64} land
# under dist/. This is the SC8/QUAL-07 verification path — no git tag required.
release-snapshot:
	goreleaser release --snapshot --clean

lld-drift:
	@bash scripts/check-lld-drift.sh

# check-no-time-now bans direct time.Now() outside the sanctioned
# internal/clock/real.go entry point. See ADR-0006 + ADR-0007.
check-no-time-now:
	@bash scripts/check-no-time-now.sh

# verify is the umbrella lint target. Plan 04-04 wired
# check-no-time-now in alongside the existing lld-drift gate; later
# phases append further checks here.
verify: lld-drift check-no-time-now

lld-drift-update:
	@{ \
		echo "=== go doc -all ./pkg/raft ==="; \
		go doc -all ./pkg/raft; \
		echo ""; \
		echo "=== go doc -all ./pkg/storage ==="; \
		go doc -all ./pkg/storage; \
		echo ""; \
		echo "=== go doc -all ./pkg/storage/storagetest ==="; \
		go doc -all ./pkg/storage/storagetest; \
		echo ""; \
		echo "=== go doc -all ./pkg/transport/inproc ==="; \
		go doc -all ./pkg/transport/inproc; \
	} > docs/lld-go-doc-golden.txt
	@echo "Regenerated docs/lld-go-doc-golden.txt — review the diff and commit alongside docs/LLD.md."
