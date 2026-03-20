export function normalizeText(value: unknown): string {
  return String(value ?? "").trim();
}

export function formatCount(value: number | null | undefined): string {
  return new Intl.NumberFormat("pt-BR").format(Number(value || 0));
}
