# ToyRaft Makefile
# More targets land in later phases (build, test, demo) — see ROADMAP.md.

.PHONY: hooks lld-drift lld-drift-update

hooks:
	@chmod +x .githooks/*
	git config core.hooksPath .githooks
	@echo "Hooks installed (pre-commit + commit-msg)"

lld-drift:
	@bash scripts/check-lld-drift.sh

lld-drift-update:
	@go doc -all ./pkg/raft > docs/lld-go-doc-golden.txt
	@echo "Regenerated docs/lld-go-doc-golden.txt — review the diff and commit alongside docs/LLD.md."
