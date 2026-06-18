# ToyRaft Makefile
# More targets land in later phases (build, test, demo) — see ROADMAP.md.

.PHONY: hooks

hooks:
	@chmod +x .githooks/*
	git config core.hooksPath .githooks
	@echo "Hooks installed (pre-commit + commit-msg)"
