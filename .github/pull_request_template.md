## Summary

<What changed and why. 1–3 sentences.>

## Linked specs / ADRs / RFCs

- ADR(s): <docs/adr/NNNN-…> or N/A
- RFC(s): <docs/rfc/NNNN-…> or N/A
- Spec(s) updated: <docs/PRD|HLD|LLD|WIRE|…> or N/A

## Checklist

- [ ] Conventional Commit subject (`feat|fix|refactor|docs|test|chore[(scope)]: …`, ≤72 chars, no trailing period)
- [ ] No spec/code drift — if behaviour diverges from `docs/`, either an ADR justifies it or the spec is updated in this PR
- [ ] If this PR changes public API shape, contract, or v1 scope: an RFC was discussed first
- [ ] If this PR makes an architectural decision: an ADR is included
- [ ] If this PR closes a phase: `.journal/M{n}.md` is included
- [ ] `make lint` clean (golangci-lint v2)
- [ ] `make test` and `make test-race` green
- [ ] Branch follows `feature/<descriptive-slug>` (no phase numbers)

## Test plan

<How to verify. Commands, scenarios, expected output.>
