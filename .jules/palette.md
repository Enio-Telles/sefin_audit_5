## 2024-03-22 - Formatting impact on micro-tasks
**Learning:** Running Prettier on entire files for micro-UX tasks (which are restricted to < 50 lines) can cause massive unrelated diffs, inflating the PR and violating constraints.
**Action:** When working on micro-UX tasks, either format only the specific modified lines manually or ensure Prettier is strictly configured to only format the lines touched by the diff to maintain the < 50 lines constraint.

## 2025-03-24 - Accessibility focus-visible styles in shadcn
**Learning:** The default `hover:text-foreground` on buttons in the Tabelas Parquet viewer doesn't provide visual feedback for keyboard navigation. We need to explicitly add `focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none` for elements like sort column buttons to be keyboard-accessible.
**Action:** When adding or auditing custom buttons inside table headers or complex data grids, ensure both `aria-label` (for screen readers) and `focus-visible` (for keyboard navigation) are included.
## 2024-04-01 - Add loading state to async export button
**Learning:** For asynchronous actions that trigger file downloads or backend processing (like exporting Excel from Parquet), providing clear visual feedback with a loading state (e.g., spinning icon and disabling the button) is crucial for UX. This prevents user confusion and double-clicking when the operation takes several seconds to complete. The codebase standard is to use `lucide-react` icons (like `Loader2`) for such feedback.
**Action:** Always check if buttons that trigger async operations have a corresponding loading state and apply the standard `lucide-react` loader if missing.
