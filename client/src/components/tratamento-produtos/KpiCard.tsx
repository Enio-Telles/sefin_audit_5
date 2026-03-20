import { formatCount } from "./formatters";

export function KpiCard({
  label,
  value,
  helper,
  accent = false,
}: {
  label: string;
  value: number;
  helper: string;
  accent?: boolean;
}) {
  return (
    <div
      className={
        accent
          ? "rounded-2xl border border-blue-500/30 bg-blue-500/10 px-4 py-3 shadow-sm"
          : "rounded-2xl border border-border/70 bg-card/95 px-4 py-3 shadow-sm"
      }
    >
      <div className={`text-[10px] font-black uppercase tracking-[0.18em] ${accent ? "text-blue-200" : "text-muted-foreground"}`}>
        {label}
      </div>
      <div className={`mt-2 text-2xl font-black ${accent ? "text-white" : "text-foreground"}`}>{formatCount(value)}</div>
      <div className={`mt-1 text-xs leading-5 ${accent ? "text-blue-100/80" : "text-muted-foreground"}`}>{helper}</div>
    </div>
  );
}
