import { Boxes, FileSpreadsheet, Loader2, RefreshCw, Sparkles } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { KpiCard } from "./KpiCard";
import type { ProdutoWorkspaceResumo } from "./types";
import { formatCount } from "./formatters";

export function ResumoTab({
  statusResumo,
  gruposRevisaoManual,
  runtimeStatusLoaded,
  availableRuntimeFiles,
  totalRuntimeFiles,
  lightAvailable,
  faissAvailable,
  activeCacheReady,
  activeCacheStale,
  fatoresMissing,
  fatoresIssuesLength,
  fatoresCriticos,
  fatoresAltos,
  downloadingRevisao,
  runtimeLoading,
  onOpenRevisao,
  onDownloadRevisao,
  onRebuildProdutos,
  onOpenFatores,
  onOpenSugestoes,
}: {
  statusResumo: ProdutoWorkspaceResumo;
  gruposRevisaoManual: number;
  runtimeStatusLoaded: boolean;
  availableRuntimeFiles: number;
  totalRuntimeFiles: number;
  lightAvailable: boolean;
  faissAvailable: boolean;
  activeCacheReady: boolean;
  activeCacheStale: boolean;
  fatoresMissing: boolean;
  fatoresIssuesLength: number;
  fatoresCriticos: number;
  fatoresAltos: number;
  downloadingRevisao: boolean;
  runtimeLoading: boolean;
  onOpenRevisao: () => void;
  onDownloadRevisao: () => void;
  onRebuildProdutos: () => void;
  onOpenFatores: () => void;
  onOpenSugestoes: () => void;
}) {
  return (
    <div className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr]">
      <Card className="border-border/70 bg-card/95 shadow-sm">
        <CardHeader className="pb-3">
          <CardTitle className="text-lg text-foreground">Panorama da fila</CardTitle>
          <CardDescription className="text-muted-foreground">
            Resumo curto para orientar a prioridade antes de voltar para a aba de revisao.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <KpiCard accent label="Pendentes" value={statusResumo.pendentes} helper="Casos que ainda pedem decisao." />
            <KpiCard label="Conflitos" value={gruposRevisaoManual} helper="Grupos que merecem triagem visual." />
            <KpiCard label="Verificados" value={statusResumo.verificados} helper="Grupos ja encerrados sem nova acao." />
            <KpiCard label="Consolidados" value={statusResumo.consolidados} helper="Resolvidos por unificacao." />
          </div>
          <div className="rounded-2xl border border-border/70 bg-accent/25 px-4 py-3 text-sm text-muted-foreground">
            A fila principal continua sendo a <span className="font-semibold text-foreground">Revisao</span>. Use este resumo so para se orientar, sem competir com a operacao.
          </div>
          <div className="flex flex-wrap gap-2">
            <Button className="gap-2 bg-blue-600 text-white hover:bg-blue-700" onClick={onOpenRevisao}>
              <Boxes className="h-4 w-4" />
              Voltar para a fila
            </Button>
            <Button variant="outline" className="gap-2" onClick={onDownloadRevisao} disabled={downloadingRevisao}>
              {downloadingRevisao ? <Loader2 className="h-4 w-4 animate-spin" /> : <FileSpreadsheet className="h-4 w-4" />}
              Excel da revisao
            </Button>
            <Button variant="outline" className="gap-2" onClick={onRebuildProdutos} disabled={runtimeLoading}>
              {runtimeLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
              Reprocessar base
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card className="border-border/70 bg-card/95 shadow-sm">
        <CardHeader className="pb-3">
          <CardTitle className="text-lg text-foreground">Estado da base</CardTitle>
          <CardDescription className="text-muted-foreground">
            Runtime, assistentes opcionais e fatores em um bloco so.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="rounded-2xl border border-border/70 bg-accent/30 px-4 py-3">
            <div className="text-[11px] font-black uppercase tracking-[0.18em] text-muted-foreground">Runtime</div>
            <div className="mt-2 text-sm text-foreground">
              {runtimeStatusLoaded ? "Fluxo atual ativo. Somente artefatos do sistema novo sao considerados." : "Carregando status do runtime..."}
            </div>
            <div className="mt-1 text-xs text-muted-foreground">
              Artefatos disponiveis: {formatCount(availableRuntimeFiles)} de {formatCount(totalRuntimeFiles)}.
            </div>
          </div>

          <div className="rounded-2xl border border-border/70 bg-accent/30 px-4 py-3">
            <div className="flex flex-wrap items-center gap-2">
              <span className="inline-flex rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-1 text-[11px] text-emerald-300">
                Leve {lightAvailable ? "disponivel" : "indisponivel"}
              </span>
              <span
                className={
                  faissAvailable
                    ? "inline-flex rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-1 text-[11px] text-emerald-300"
                    : "inline-flex rounded-full border border-border/70 bg-background/60 px-2 py-1 text-[11px] text-muted-foreground"
                }
              >
                {faissAvailable ? "FAISS disponivel" : "FAISS indisponivel"}
              </span>
              <span
                className={
                  activeCacheReady
                    ? activeCacheStale
                      ? "inline-flex rounded-full border border-orange-500/30 bg-orange-500/10 px-2 py-1 text-[11px] text-orange-300"
                      : "inline-flex rounded-full border border-blue-500/30 bg-blue-500/10 px-2 py-1 text-[11px] text-blue-300"
                    : "inline-flex rounded-full border border-border/70 bg-background/60 px-2 py-1 text-[11px] text-muted-foreground"
                }
              >
                {activeCacheReady ? (activeCacheStale ? "cache desatualizado" : "cache pronto") : "sem cache"}
              </span>
            </div>
            <div className="mt-2 text-xs leading-5 text-muted-foreground">
              O assistente de sugestoes continua opcional e so consome desempenho quando for executado.
            </div>
          </div>

          <div className="rounded-2xl border border-border/70 bg-accent/30 px-4 py-3">
            <div className="text-[11px] font-black uppercase tracking-[0.18em] text-muted-foreground">Fatores</div>
            <div className="mt-2 text-sm text-foreground">
              {fatoresMissing ? "Ainda nao calculados para este CNPJ." : `${formatCount(fatoresIssuesLength)} ocorrencias operacionais encontradas.`}
            </div>
            <div className="mt-1 text-xs text-muted-foreground">
              Criticos: {formatCount(fatoresCriticos)}. Altos: {formatCount(fatoresAltos)}.
            </div>
            <div className="mt-3 flex flex-wrap gap-2">
              <Button variant="outline" className="gap-2" onClick={onOpenFatores}>
                <FileSpreadsheet className="h-4 w-4" />
                Abrir fatores
              </Button>
              <Button variant="outline" className="gap-2" onClick={onOpenSugestoes}>
                <Sparkles className="h-4 w-4" />
                Abrir sugestoes
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
