## 2024-05-18 - Accessibility for icon-only buttons
**Learning:** Found multiple instances where `Button` component with `size="icon"` lacked `aria-label` and `title` attributes. Without them, screen readers simply read "button" instead of its function (e.g., "Voltar" or "Enviar mensagem"). This makes the app highly inaccessible.
**Action:** Always ensure that icon-only buttons have both an `aria-label` (for screen readers) and a `title` (for visual users who need tooltips).
