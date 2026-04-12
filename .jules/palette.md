## 2024-03-22 - Formatting impact on micro-tasks
**Learning:** Running Prettier on entire files for micro-UX tasks (which are restricted to < 50 lines) can cause massive unrelated diffs, inflating the PR and violating constraints.
**Action:** When working on micro-UX tasks, either format only the specific modified lines manually or ensure Prettier is strictly configured to only format the lines touched by the diff to maintain the < 50 lines constraint.

## 2025-03-24 - Accessibility focus-visible styles in shadcn
**Learning:** The default `hover:text-foreground` on buttons in the Tabelas Parquet viewer doesn't provide visual feedback for keyboard navigation. We need to explicitly add `focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none` for elements like sort column buttons to be keyboard-accessible.
**Action:** When adding or auditing custom buttons inside table headers or complex data grids, ensure both `aria-label` (for screen readers) and `focus-visible` (for keyboard navigation) are included.
## 2025-04-12 - Improve accessibility of extraction settings toggle
**Learning:** For `Switch` and other form elements constructed visually next to a `Label`, the text description often acts as a visual label but lacks programmatic association. Clicking the label text doesn't activate the switch by default without `id` and `htmlFor`.
**Action:** Always ensure that `Label` components use `htmlFor` matching the `id` of the interactive element (`Switch`, `Checkbox`, `Input`) they describe. This dramatically improves both screen reader accessibility and the click target area for mouse/touch users.
