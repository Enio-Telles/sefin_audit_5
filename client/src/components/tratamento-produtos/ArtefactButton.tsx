import { ExternalLink } from "lucide-react";

export function ArtefactButton({
  available,
  helper,
  label,
  onClick,
}: {
  available: boolean;
  helper: string;
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={!available}
      className={
        available
          ? "flex w-full items-start justify-between gap-3 rounded-xl border border-border/70 bg-card/80 px-3 py-3 text-left transition hover:border-primary/40 hover:bg-accent/40"
          : "flex w-full items-start justify-between gap-3 rounded-xl border border-border/50 bg-background/40 px-3 py-3 text-left opacity-60"
      }
    >
      <div>
        <div className="text-sm font-semibold text-foreground">{label}</div>
        <div className="mt-1 text-xs leading-5 text-muted-foreground">{helper}</div>
      </div>
      <ExternalLink className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />
    </button>
  );
}
