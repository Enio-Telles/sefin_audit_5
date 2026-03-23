## 2024-03-22 - Formatting impact on micro-tasks
**Learning:** Running Prettier on entire files for micro-UX tasks (which are restricted to < 50 lines) can cause massive unrelated diffs, inflating the PR and violating constraints.
**Action:** When working on micro-UX tasks, either format only the specific modified lines manually or ensure Prettier is strictly configured to only format the lines touched by the diff to maintain the < 50 lines constraint.
