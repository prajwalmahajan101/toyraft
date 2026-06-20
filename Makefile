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
	@{ \
		echo "=== go doc -all ./pkg/raft ==="; \
		go doc -all ./pkg/raft; \
		echo ""; \
		echo "=== go doc -all ./pkg/storage ==="; \
		go doc -all ./pkg/storage; \
		echo ""; \
		echo "=== go doc -all ./pkg/storage/storagetest ==="; \
		go doc -all ./pkg/storage/storagetest; \
	} > docs/lld-go-doc-golden.txt
	@echo "Regenerated docs/lld-go-doc-golden.txt — review the diff and commit alongside docs/LLD.md."
