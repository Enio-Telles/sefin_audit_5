## 2024-05-18 - Accessibility for icon-only buttons
**Learning:** Found multiple instances where `Button` component with `size="icon"` lacked `aria-label` and `title` attributes. Without them, screen readers simply read "button" instead of its function (e.g., "Voltar" or "Enviar mensagem"). This makes the app highly inaccessible.
**Action:** Always ensure that icon-only buttons have both an `aria-label` (for screen readers) and a `title` (for visual users who need tooltips).

## 2024-05-19 - Added ARIA label to User Profile Menu Dropdown Button
**Learning:** Found a button triggering the user profile dropdown (`client/src/components/DashboardLayout.tsx`) containing dynamic text (`user?.name`, `user?.email`), but when collapsed, it renders icon/initials only (no descriptive text visible).
**Action:** Always add an `aria-label` (e.g., `aria-label="Menu do usuário"`) or an `sr-only` span text to buttons whose visible text dynamically disappears on state changes (e.g., when a sidebar collapses). This guarantees consistent context for screen readers in any view state.
