import { ExternalLink, Loader2, RefreshCw, Wand2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { formatCount } from "./formatters";

export function SugestoesTab({
  suggestionMode,
  setSuggestionMode,
  suggestionTopK,
  setSuggestionTopK,
  suggestionMinScore,
  setSuggestionMinScore,
  suggestionLoading,
  faissAvailable,
  suggestionFilePath,
  activeSummaryLoading,
  activeVisibleCount,
  activeFileCount,
  modeMessage,
  onGenerateSuggestions,
  onOpenSuggestionFile,
  onClearSuggestions,
}: {
  suggestionMode: "off" | "light" | "faiss";
  setSuggestionMode: (value: "off" | "light" | "faiss") => void;
  suggestionTopK: string;
  setSuggestionTopK: (value: string) => void;
  suggestionMinScore: string;
  setSuggestionMinScore: (value: string) => void;
  suggestionLoading: boolean;
  faissAvailable: boolean;
  suggestionFilePath: string;
  activeSummaryLoading: boolean;
  activeVisibleCount: number;
  activeFileCount: number;
  modeMessage: string;
  onGenerateSuggestions: () => void;
  onOpenSuggestionFile: () => void;
  onClearSuggestions: () => void;
}) {
  return (
    <Card className="border-border/70 bg-card/95 shadow-sm">
      <CardHeader className="pb-3">
        <CardTitle className="text-lg text-foreground">Sugestoes opcionais</CardTitle>
        <CardDescription className="text-muted-foreground">
          Use similaridade textual so quando quiser apoio para descobrir grupos que talvez devam ser agregados ou mantidos separados.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid gap-3 lg:grid-cols-[180px_110px_130px_auto_auto_auto]">
          <div className="space-y-1.5">
            <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">Modo</div>
            <Select value={suggestionMode} onValueChange={(value: "off" | "light" | "faiss") => setSuggestionMode(value)}>
              <SelectTrigger className="bg-background/60">
                <SelectValue placeholder="Selecione" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="off">Desligado</SelectItem>
                <SelectItem value="light">Leve</SelectItem>
                <SelectItem value="faiss">FAISS</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1.5">
            <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">Top K</div>
            <Input value={suggestionTopK} onChange={(event) => setSuggestionTopK(event.target.value)} className="bg-background/60" inputMode="numeric" />
          </div>
          <div className="space-y-1.5">
            <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">Score min.</div>
            <Input
              value={suggestionMinScore}
              onChange={(event) => setSuggestionMinScore(event.target.value)}
              className="bg-background/60"
              inputMode="decimal"
            />
          </div>
          <Button
            className="gap-2 self-end bg-blue-600 text-white hover:bg-blue-700"
            onClick={onGenerateSuggestions}
            disabled={suggestionLoading || suggestionMode === "off" || (suggestionMode === "faiss" && !faissAvailable)}
          >
            {suggestionLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Wand2 className="h-4 w-4" />}
            Gerar sugestoes
          </Button>
          <Button variant="outline" className="gap-2 self-end" onClick={onOpenSuggestionFile} disabled={!suggestionFilePath}>
            <ExternalLink className="h-4 w-4" />
            Abrir arquivo
          </Button>
          <Button variant="ghost" className="gap-2 self-end" onClick={onClearSuggestions} disabled={suggestionLoading || suggestionMode === "off"}>
            <RefreshCw className="h-4 w-4" />
            Limpar cache
          </Button>
        </div>

        <div className="rounded-2xl border border-border/70 bg-accent/30 px-4 py-3 text-sm text-muted-foreground">
          O modo <span className="font-semibold text-foreground">{suggestionMode === "off" ? "Desligado" : suggestionMode === "faiss" ? "FAISS" : "Leve"}</span>{" "}
          permanece opcional e nao interfere na tabela final enquanto voce nao o executar.
        </div>

        <div className="grid gap-3 lg:grid-cols-3">
          <div className="rounded-2xl border border-border/70 bg-background/50 px-4 py-3">
            <div className="text-[11px] font-black uppercase tracking-[0.18em] text-muted-foreground">Disponibilidade</div>
            <div className="mt-2 text-sm text-foreground">{modeMessage}</div>
          </div>
          <div className="rounded-2xl border border-border/70 bg-background/50 px-4 py-3">
            <div className="text-[11px] font-black uppercase tracking-[0.18em] text-muted-foreground">Fila operacional</div>
            <div className="mt-2 text-sm text-foreground">
              {activeSummaryLoading ? "Atualizando contagens..." : `${formatCount(activeVisibleCount)} visiveis agora`}
            </div>
            <div className="mt-1 text-xs text-muted-foreground">O total visivel exclui pares escondidos por status.</div>
          </div>
          <div className="rounded-2xl border border-border/70 bg-background/50 px-4 py-3">
            <div className="text-[11px] font-black uppercase tracking-[0.18em] text-muted-foreground">Arquivo bruto</div>
            <div className="mt-2 text-sm text-foreground">{formatCount(activeFileCount)}</div>
            <div className="mt-1 text-xs text-muted-foreground">Corresponde ao parquet salvo no disco.</div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
