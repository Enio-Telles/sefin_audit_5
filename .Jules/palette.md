## 2024-05-18 - Accessibility for icon-only buttons
**Learning:** Found multiple instances where `Button` component with `size="icon"` lacked `aria-label` and `title` attributes. Without them, screen readers simply read "button" instead of its function (e.g., "Voltar" or "Enviar mensagem"). This makes the app highly inaccessible.
**Action:** Always ensure that icon-only buttons have both an `aria-label` (for screen readers) and a `title` (for visual users who need tooltips).
## 2026-03-17 - Keyboard Accessibility for Hover-only Actions
**Learning:** In the Tabelas component, many icon-only buttons (like row/column options and file operations) relied on 'opacity-0 group-hover:opacity-100' for visual clutter reduction. However, this pattern inherently hides them from keyboard users who use tab navigation, creating a severe accessibility barrier.
**Action:** When implementing 'appear-on-hover' UI elements, always combine them with 'focus-visible:opacity-100' and a clear focus ring ('focus-visible:ring-2') so they remain navigable and usable via keyboard without needing mouse interaction.
