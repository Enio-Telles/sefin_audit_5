## 2024-03-22 - Formatting impact on micro-tasks
**Learning:** Running Prettier on entire files for micro-UX tasks (which are restricted to < 50 lines) can cause massive unrelated diffs, inflating the PR and violating constraints.
**Action:** When working on micro-UX tasks, either format only the specific modified lines manually or ensure Prettier is strictly configured to only format the lines touched by the diff to maintain the < 50 lines constraint.

## 2025-03-24 - Accessibility focus-visible styles in shadcn
**Learning:** The default `hover:text-foreground` on buttons in the Tabelas Parquet viewer doesn't provide visual feedback for keyboard navigation. We need to explicitly add `focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none` for elements like sort column buttons to be keyboard-accessible.
**Action:** When adding or auditing custom buttons inside table headers or complex data grids, ensure both `aria-label` (for screen readers) and `focus-visible` (for keyboard navigation) are included.

## 2024-03-24 - Table Header Accessibility
**Learning:** Table headers with sort functionality often use raw `onClick` handlers on `<th>` elements (`<TableHead>`), which is inaccessible to keyboard navigation and screen readers.
**Action:** When making table headers interactive for sorting, wrap the header text in a full-width, full-height `<button type="button">`, remove the `cursor-pointer` from the `<th>`, and apply `focus-visible` styles and an appropriate `aria-label` to the button instead.
