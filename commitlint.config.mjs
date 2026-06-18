// commitlint config — Conventional Commits, relaxed body wrapping.
//
// We enforce the subject shape (type, scope, length, no trailing period) but
// relax body-max-line-length because long lines are common in bodies that
// quote tool output, commands, or rationale.
export default {
  extends: ["@commitlint/config-conventional"],
  rules: {
    "body-max-line-length": [0, "always", 100],
    "footer-max-line-length": [0, "always", 100],
    "subject-case": [0],
  },
};
