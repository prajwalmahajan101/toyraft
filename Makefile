# ToyRaft Makefile
# More targets land in later phases (build, test, demo) — see ROADMAP.md.

.PHONY: hooks lld-drift lld-drift-update check-no-time-now verify

hooks:
	@chmod +x .githooks/*
	git config core.hooksPath .githooks
	@echo "Hooks installed (pre-commit + commit-msg)"

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
