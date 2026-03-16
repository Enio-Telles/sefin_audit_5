import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useLocation } from "wouter";
import {
  Boxes,
  ChevronLeft,
  ExternalLink,
  FileSpreadsheet,
  FolderOpen,
  GitBranch,
  Loader2,
  RefreshCw,
  Settings2,
  Sparkles,
  Table2,
  Wand2,
} from "lucide-react";
import { toast } from "sonner";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { RevisaoFinalProdutosView } from "@/pages/RevisaoFinalProdutos";
import {
  clearVectorizacaoCache,
  diagnosticarFatoresConversao,
  downloadRevisaoManualExcel,
  getParesGruposSimilares,
  getProdutosRevisaoFinal,
  getRuntimeProdutosStatus,
  getStatusAnaliseProdutos,
  getVectorizacaoStatus,
  rebuildRuntimeProdutos,
  type ProdutoAnaliseStatusResumo,
} from "@/lib/pythonApi";

type WorkspaceTab = "revisao" | "resumo" | "sugestoes" | "fatores" | "avancado";

function normalizeText(value: unknown): string {
  return String(value ?? "").trim();
}

function formatCount(value: number | null | undefined): string {
  return new Intl.NumberFormat("pt-BR").format(Number(value || 0));
}

function KpiCard({
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

function ArtefactButton({
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

export default function AnaliseProdutos() {
  const [, navigate] = useLocation();
  const searchParams = new URLSearchParams(window.location.search);
  const cnpj = normalizeText(searchParams.get("cnpj")).replace(/\D/g, "");
  const initialTab = ((): WorkspaceTab => {
    const tab = normalizeText(searchParams.get("tab")).toLowerCase();
    if (tab === "resumo" || tab === "sugestoes" || tab === "fatores" || tab === "avancado") return tab;
    return "revisao";
  })();

  const [activeTab, setActiveTab] = useState<WorkspaceTab>(initialTab);
  const [downloadingRevisao, setDownloadingRevisao] = useState(false);
  const [runtimeLoading, setRuntimeLoading] = useState(false);
  const [suggestionMode, setSuggestionMode] = useState<"off" | "light" | "faiss">("off");
  const [suggestionTopK, setSuggestionTopK] = useState("8");
  const [suggestionMinScore, setSuggestionMinScore] = useState("0.72");
  const [suggestionLoading, setSuggestionLoading] = useState(false);

  const emptyStatusResumo = useMemo<ProdutoAnaliseStatusResumo>(
    () => ({
      pendentes: 0,
      verificados: 0,
      consolidados: 0,
      separados: 0,
      decididos_entre_grupos: 0,
    }),
    []
  );

  const metaQuery = useQuery({
    queryKey: ["analise-produtos-meta", cnpj],
    queryFn: () => getProdutosRevisaoFinal(cnpj),
    enabled: Boolean(cnpj),
    staleTime: 30_000,
    retry: false,
  });

  const statusQuery = useQuery({
    queryKey: ["analise-produtos-status", cnpj],
    queryFn: () =>
      getStatusAnaliseProdutos(cnpj, { includeData: false }).catch(() => ({
        success: true,
        file_path: "",
        data: [],
        resumo: emptyStatusResumo,
      })),
    enabled: Boolean(cnpj),
    staleTime: 30_000,
    retry: 1,
  });

  const runtimeQuery = useQuery({
    queryKey: ["analise-produtos-runtime", cnpj],
    queryFn: () => getRuntimeProdutosStatus(cnpj),
    enabled: Boolean(cnpj),
    staleTime: 30_000,
    retry: 1,
  });

  const vectorStatusQuery = useQuery({
    queryKey: ["analise-produtos-vector", cnpj],
    queryFn: () => getVectorizacaoStatus(cnpj),
    enabled: Boolean(cnpj),
    staleTime: 30_000,
    retry: 1,
  });

  const fatoresQuery = useQuery({
    queryKey: ["analise-produtos-fatores", cnpj],
    queryFn: () => diagnosticarFatoresConversao(cnpj),
    enabled: Boolean(cnpj),
    staleTime: 30_000,
    retry: false,
  });

  const statusResumo = statusQuery.data?.resumo || emptyStatusResumo;
  const runtimeStatus = runtimeQuery.data?.runtime || null;
  const factorsError = fatoresQuery.error as (Error & { status?: number }) | null;
  const fatoresMissing = fatoresQuery.data ? !fatoresQuery.data.available : factorsError?.status === 404;
  const fatoresIssues = fatoresQuery.data?.issues || [];
  const fatoresCriticos = fatoresIssues.filter((item) => normalizeText(item.severidade).toLowerCase() === "critico").length;
  const fatoresAltos = fatoresIssues.filter((item) => normalizeText(item.severidade).toLowerCase() === "alto").length;

  const faissModeStatus = vectorStatusQuery.data?.status?.modes?.faiss;
  const lightModeStatus = vectorStatusQuery.data?.status?.modes?.light;
  const faissCache = vectorStatusQuery.data?.caches?.faiss;
  const lightCache = vectorStatusQuery.data?.caches?.light;
  const activeCache = suggestionMode === "faiss" ? faissCache : suggestionMode === "light" ? lightCache : null;

  const activeSummaryQuery = useQuery({
    queryKey: [
      "analise-produtos-suggestion-summary",
      cnpj,
      suggestionMode,
      activeCache?.generated_at_utc ?? "",
      suggestionTopK,
      suggestionMinScore,
    ],
    queryFn: () =>
      getParesGruposSimilares(cnpj, suggestionMode === "faiss" ? "faiss" : "light", false, {
        topK: Math.max(2, Math.min(20, Number(suggestionTopK) || 8)),
        minScore: Math.max(0.3, Math.min(0.98, Number(suggestionMinScore) || (suggestionMode === "faiss" ? 0.62 : 0.72))),
        page: 1,
        pageSize: 1,
        showAnalyzed: false,
      }),
    enabled: Boolean(cnpj && suggestionMode !== "off" && activeCache?.generated_at_utc),
    staleTime: 30_000,
    retry: 1,
  });

  const openParquetInNewTab = (filePath: string) => {
    const normalized = normalizeText(filePath).replace(/\\/g, "/");
    if (!normalized) return;
    window.open(`/tabelas/view?file_path=${encodeURIComponent(normalized)}`, "_blank");
  };

  const openWorkspaceTab = (tab: WorkspaceTab, options?: { agrupamento?: "faiss" | "flat" | null }) => {
    const params = new URLSearchParams(window.location.search);
    params.set("cnpj", cnpj);
    params.set("tab", tab);
    if (options && "agrupamento" in options) {
      if (options.agrupamento) params.set("agrupamento", options.agrupamento);
      else params.delete("agrupamento");
    }
    navigate(`/analise-produtos?${params.toString()}`);
    setActiveTab(tab);
  };

  const openRevisaoFinal = () => {
    openWorkspaceTab("revisao", {
      agrupamento: suggestionMode === "faiss" && faissCache?.generated_at_utc && !faissCache?.stale ? "faiss" : null,
    });
  };

  const openRevisaoFatores = () => {
    navigate(`/revisao-fatores?cnpj=${cnpj}`);
  };

  const handleDownloadRevisao = async () => {
    if (!cnpj) return;
    setDownloadingRevisao(true);
    try {
      await downloadRevisaoManualExcel(cnpj);
      toast.success("Planilha de revisao final baixada.");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Falha ao baixar a planilha.");
    } finally {
      setDownloadingRevisao(false);
    }
  };

  const handleRebuildProdutos = async () => {
    if (!cnpj) return;
    setRuntimeLoading(true);
    try {
      const response = await rebuildRuntimeProdutos(cnpj);
      toast.success("Pipeline de produtos reprocessado.", {
        description: `${formatCount(response.rows)} grupos atualizados no runtime.`,
      });
      await Promise.all([metaQuery.refetch(), statusQuery.refetch(), runtimeQuery.refetch()]);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Falha ao reprocessar a base.");
    } finally {
      setRuntimeLoading(false);
    }
  };

  const handleGenerateSuggestions = async () => {
    if (!cnpj || suggestionMode === "off") return;
    setSuggestionLoading(true);
    try {
      const response = await getParesGruposSimilares(cnpj, suggestionMode === "faiss" ? "faiss" : "light", true, {
        topK: Math.max(2, Math.min(20, Number(suggestionTopK) || 8)),
        minScore: Math.max(0.3, Math.min(0.98, Number(suggestionMinScore) || (suggestionMode === "faiss" ? 0.62 : 0.72))),
        page: 1,
        pageSize: 25,
        showAnalyzed: false,
      });
      toast.success(`Sugestoes ${suggestionMode === "faiss" ? "FAISS" : "leves"} geradas.`, {
        description: `${formatCount(Number(response.total_filtered ?? response.total ?? 0))} visiveis de ${formatCount(
          Number(response.total_file ?? response.total ?? 0)
        )} no arquivo.`,
      });
      await Promise.all([vectorStatusQuery.refetch(), activeSummaryQuery.refetch()]);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Falha ao gerar sugestoes.");
    } finally {
      setSuggestionLoading(false);
    }
  };

  const handleClearSuggestions = async () => {
    if (!cnpj || suggestionMode === "off") return;
    setSuggestionLoading(true);
    try {
      await clearVectorizacaoCache(cnpj, suggestionMode === "faiss" ? "faiss" : "light");
      toast.success("Cache de sugestoes removido.");
      await Promise.all([vectorStatusQuery.refetch(), activeSummaryQuery.refetch()]);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Falha ao limpar cache.");
    } finally {
      setSuggestionLoading(false);
    }
  };

  const runtimeFiles = runtimeStatus?.files || {};
  const totalRuntimeFiles = Object.keys(runtimeFiles).length;
  const availableRuntimeFiles = Object.values(runtimeFiles).filter((item) => item.exists).length;
  const suggestionFilePath = normalizeText(activeSummaryQuery.data?.file_path || "");

  if (!cnpj) {
    return (
      <div className="container mx-auto py-6">
        <Empty className="min-h-[60vh] border border-border/70 bg-card/90">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <Boxes />
            </EmptyMedia>
            <EmptyTitle>Analise de produtos sem CNPJ</EmptyTitle>
            <EmptyDescription>
              Abra esta tela a partir da auditoria para entrar no workspace principal da analise de produtos.
            </EmptyDescription>
          </EmptyHeader>
          <Button onClick={() => navigate("/auditar")}>Ir para auditoria</Button>
        </Empty>
      </div>
    );
  }

  return (
    <div className="container mx-auto max-w-7xl space-y-6 py-6">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="space-y-3">
          <Button variant="ghost" className="-ml-3 w-fit gap-2 text-muted-foreground" onClick={() => navigate("/auditar")}>
            <ChevronLeft className="h-4 w-4" />
            Voltar para auditoria
          </Button>
          <div className="space-y-2">
            <div className="flex flex-wrap items-center gap-2">
              <h1 className="text-3xl font-black tracking-tight text-foreground">Analise de Produtos</h1>
              <Badge variant="outline" className="border-blue-500/30 bg-blue-500/10 text-blue-200">
                {cnpj}
              </Badge>
            </div>
            <p className="max-w-3xl text-sm leading-6 text-muted-foreground">
              Workspace principal para revisar a tabela final, consultar sugestoes opcionais e acompanhar os fatores sem sair da mesma jornada.
            </p>
          </div>
        </div>

        <div className="grid gap-2 sm:grid-cols-2">
          <Button className="gap-2 bg-blue-600 text-white hover:bg-blue-700" onClick={openRevisaoFinal}>
            <Boxes className="h-4 w-4" />
            Abrir revisao final
          </Button>
          <Button variant="outline" className="gap-2" onClick={() => openWorkspaceTab("fatores")}>
            <FileSpreadsheet className="h-4 w-4" />
            Ver fatores
          </Button>
        </div>
      </div>

      <div className="flex flex-wrap gap-2">
        <Badge className="border border-blue-500/30 bg-blue-500/10 text-blue-200 hover:bg-blue-500/10">
          Pendentes: {formatCount(statusResumo.pendentes)}
        </Badge>
        <Badge className="border border-amber-500/30 bg-amber-500/10 text-amber-200 hover:bg-amber-500/10">
          Conflitos: {formatCount(metaQuery.data?.summary.grupos_revisao_manual || 0)}
        </Badge>
        <Badge className="border border-emerald-500/30 bg-emerald-500/10 text-emerald-200 hover:bg-emerald-500/10">
          Verificados: {formatCount(statusResumo.verificados)}
        </Badge>
        <Badge variant="outline" className="border-border/70 bg-card/80 text-muted-foreground">
          Total grupos: {formatCount(metaQuery.data?.summary.total_grupos || 0)}
        </Badge>
      </div>

      <Tabs value={activeTab} onValueChange={(value) => openWorkspaceTab(value as WorkspaceTab)} className="space-y-4">
        <TabsList className="w-full justify-start gap-2 overflow-x-auto rounded-2xl border border-border/70 bg-card/90 p-1">
          <TabsTrigger value="revisao">Revisao</TabsTrigger>
          <TabsTrigger value="resumo">Resumo</TabsTrigger>
          <TabsTrigger value="sugestoes">Sugestoes</TabsTrigger>
          <TabsTrigger value="fatores">Fatores</TabsTrigger>
          <TabsTrigger value="avancado">Avancado</TabsTrigger>
        </TabsList>

        <TabsContent value="revisao" className="space-y-4">
          <RevisaoFinalProdutosView embedded />
        </TabsContent>

        <TabsContent value="resumo" className="space-y-4">
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
                  <KpiCard label="Conflitos" value={metaQuery.data?.summary.grupos_revisao_manual || 0} helper="Grupos que merecem triagem visual." />
                  <KpiCard label="Verificados" value={statusResumo.verificados} helper="Grupos ja encerrados sem nova acao." />
                  <KpiCard label="Consolidados" value={statusResumo.consolidados} helper="Resolvidos por unificacao." />
                </div>
                <div className="rounded-2xl border border-border/70 bg-accent/25 px-4 py-3 text-sm text-muted-foreground">
                  A fila principal continua sendo a <span className="font-semibold text-foreground">Revisao</span>. Use este resumo so para se orientar, sem competir com a operacao.
                </div>
                <div className="flex flex-wrap gap-2">
                  <Button className="gap-2 bg-blue-600 text-white hover:bg-blue-700" onClick={openRevisaoFinal}>
                    <Boxes className="h-4 w-4" />
                    Voltar para a fila
                  </Button>
                  <Button variant="outline" className="gap-2" onClick={handleDownloadRevisao} disabled={downloadingRevisao}>
                    {downloadingRevisao ? <Loader2 className="h-4 w-4 animate-spin" /> : <FileSpreadsheet className="h-4 w-4" />}
                    Excel da revisao
                  </Button>
                  <Button variant="outline" className="gap-2" onClick={handleRebuildProdutos} disabled={runtimeLoading}>
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
                    {runtimeStatus ? "Fluxo atual ativo. Somente artefatos do sistema novo sao considerados." : "Carregando status do runtime..."}
                  </div>
                  <div className="mt-1 text-xs text-muted-foreground">
                    Artefatos disponiveis: {formatCount(availableRuntimeFiles)} de {formatCount(totalRuntimeFiles)}.
                  </div>
                </div>

                <div className="rounded-2xl border border-border/70 bg-accent/30 px-4 py-3">
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant="outline" className="border-emerald-500/30 bg-emerald-500/10 text-emerald-300">
                      Leve {lightModeStatus?.available === false ? "indisponivel" : "disponivel"}
                    </Badge>
                    <Badge
                      variant="outline"
                      className={
                        faissModeStatus?.available
                          ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-300"
                          : "border-border/70 bg-background/60 text-muted-foreground"
                      }
                    >
                      {faissModeStatus?.available ? "FAISS disponivel" : "FAISS indisponivel"}
                    </Badge>
                    <Badge
                      variant="outline"
                      className={
                        activeCache?.generated_at_utc
                          ? activeCache?.stale
                            ? "border-orange-500/30 bg-orange-500/10 text-orange-300"
                            : "border-blue-500/30 bg-blue-500/10 text-blue-300"
                          : "border-border/70 bg-background/60 text-muted-foreground"
                      }
                    >
                      {activeCache?.generated_at_utc ? (activeCache?.stale ? "cache desatualizado" : "cache pronto") : "sem cache"}
                    </Badge>
                  </div>
                  <div className="mt-2 text-xs leading-5 text-muted-foreground">
                    O assistente de sugestoes continua opcional e so consome desempenho quando for executado.
                  </div>
                </div>

                <div className="rounded-2xl border border-border/70 bg-accent/30 px-4 py-3">
                  <div className="text-[11px] font-black uppercase tracking-[0.18em] text-muted-foreground">Fatores</div>
                  <div className="mt-2 text-sm text-foreground">
                    {fatoresMissing ? "Ainda nao calculados para este CNPJ." : `${formatCount(fatoresIssues.length)} ocorrencias operacionais encontradas.`}
                  </div>
                  <div className="mt-1 text-xs text-muted-foreground">
                    Criticos: {formatCount(fatoresCriticos)}. Altos: {formatCount(fatoresAltos)}.
                  </div>
                  <div className="mt-3 flex flex-wrap gap-2">
                    <Button variant="outline" className="gap-2" onClick={() => openWorkspaceTab("fatores")}>
                      <FileSpreadsheet className="h-4 w-4" />
                      Abrir fatores
                    </Button>
                    <Button variant="outline" className="gap-2" onClick={() => openWorkspaceTab("sugestoes")}>
                      <Sparkles className="h-4 w-4" />
                      Abrir sugestoes
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        <TabsContent value="sugestoes" className="space-y-4">
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
                  onClick={handleGenerateSuggestions}
                  disabled={suggestionLoading || suggestionMode === "off" || (suggestionMode === "faiss" && !faissModeStatus?.available)}
                >
                  {suggestionLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Wand2 className="h-4 w-4" />}
                  Gerar sugestoes
                </Button>
                <Button variant="outline" className="gap-2 self-end" onClick={() => openParquetInNewTab(suggestionFilePath)} disabled={!suggestionFilePath}>
                  <ExternalLink className="h-4 w-4" />
                  Abrir arquivo
                </Button>
                <Button variant="ghost" className="gap-2 self-end" onClick={handleClearSuggestions} disabled={suggestionLoading || suggestionMode === "off"}>
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
                  <div className="mt-2 text-sm text-foreground">
                    {suggestionMode === "faiss"
                      ? faissModeStatus?.message || "FAISS so roda quando solicitado."
                      : lightModeStatus?.message || "Modo leve pronto para uso sob demanda."}
                  </div>
                </div>
                <div className="rounded-2xl border border-border/70 bg-background/50 px-4 py-3">
                  <div className="text-[11px] font-black uppercase tracking-[0.18em] text-muted-foreground">Fila operacional</div>
                  <div className="mt-2 text-sm text-foreground">
                    {activeSummaryQuery.isLoading
                      ? "Atualizando contagens..."
                      : `${formatCount(Number(activeSummaryQuery.data?.total_filtered ?? activeSummaryQuery.data?.total ?? 0))} visiveis agora`}
                  </div>
                  <div className="mt-1 text-xs text-muted-foreground">O total visivel exclui pares escondidos por status.</div>
                </div>
                <div className="rounded-2xl border border-border/70 bg-background/50 px-4 py-3">
                  <div className="text-[11px] font-black uppercase tracking-[0.18em] text-muted-foreground">Arquivo bruto</div>
                  <div className="mt-2 text-sm text-foreground">
                    {formatCount(Number(activeSummaryQuery.data?.total_file ?? activeSummaryQuery.data?.total ?? 0))}
                  </div>
                  <div className="mt-1 text-xs text-muted-foreground">Corresponde ao parquet salvo no disco.</div>
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="fatores" className="space-y-4">
          <Card className="border-border/70 bg-card/95 shadow-sm">
            <CardHeader className="pb-3">
              <CardTitle className="text-lg text-foreground">Fatores de conversao</CardTitle>
              <CardDescription className="text-muted-foreground">
                A revisao de fatores continua separada na operacao detalhada, mas agora nasce dentro da mesma jornada.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              {fatoresMissing ? (
                <Alert className="border-amber-500/30 bg-amber-500/10">
                  <AlertTitle>Fatores ainda nao calculados</AlertTitle>
                  <AlertDescription>
                    Rode o calculo quando quiser abrir a fila operacional de unidades e conversoes.
                  </AlertDescription>
                </Alert>
              ) : null}

              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                <KpiCard label="Ocorrencias" value={fatoresIssues.length} helper="Total de alertas operacionais encontrados." accent />
                <KpiCard label="Criticos" value={fatoresCriticos} helper="Fatores invalidos ou extremos com maior risco." />
                <KpiCard label="Altos" value={fatoresAltos} helper="Casos relevantes, mas abaixo do nivel critico." />
                <KpiCard
                  label="Registros"
                  value={fatoresQuery.data?.stats.total_registros || 0}
                  helper="Total de linhas analisadas no parquet de fatores."
                />
              </div>

              <div className="flex flex-wrap gap-2">
                <Button className="gap-2 bg-blue-600 text-white hover:bg-blue-700" onClick={openRevisaoFatores}>
                  <FileSpreadsheet className="h-4 w-4" />
                  Abrir revisao de fatores
                </Button>
                <Button variant="outline" className="gap-2" onClick={() => openParquetInNewTab(fatoresQuery.data?.file || "")} disabled={!fatoresQuery.data?.file}>
                  <ExternalLink className="h-4 w-4" />
                  Abrir parquet de fatores
                </Button>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="avancado" className="space-y-4">
          <Card className="border-border/70 bg-card/95 shadow-sm">
            <CardHeader className="pb-3">
              <CardTitle className="flex items-center gap-2 text-lg text-foreground">
                <Settings2 className="h-4 w-4 text-muted-foreground" />
                Camada tecnica
              </CardTitle>
              <CardDescription className="text-muted-foreground">
                Arquivos e atalhos de apoio continuam disponiveis, mas fora do caminho principal do analista.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid gap-3 md:grid-cols-2">
                <ArtefactButton
                  available={Boolean(metaQuery.data?.file_path)}
                  label="Tabela final"
                  helper="Arquivo principal da revisao final ja desagregada."
                  onClick={() => openParquetInNewTab(metaQuery.data?.file_path || "")}
                />
                <ArtefactButton
                  available={Boolean(runtimeFiles.status_analise?.exists)}
                  label="Status de analise"
                  helper="Historico operacional de verificados, consolidados e separados."
                  onClick={() => openParquetInNewTab(String(runtimeFiles.status_analise?.path || ""))}
                />
                <ArtefactButton
                  available={Boolean(runtimeFiles.base_detalhes?.exists)}
                  label="Base de detalhes"
                  helper="Camada mais tecnica, usada para rastrear descricoes e campos brutos."
                  onClick={() => openParquetInNewTab(String(runtimeFiles.base_detalhes?.path || ""))}
                />
                <ArtefactButton
                  available={Boolean(runtimeFiles.mapa_agregados?.exists)}
                  label="Mapa de agregados"
                  helper="Rastreamento das decisoes de agregacao que alimentam a tabela final."
                  onClick={() => openParquetInNewTab(String(runtimeFiles.mapa_agregados?.path || ""))}
                />
                <ArtefactButton
                  available={Boolean(runtimeFiles.mapa_desagregados?.exists)}
                  label="Mapa de desagregados"
                  helper="Rastreamento das separacoes de codigo aplicadas antes da tabela final."
                  onClick={() => openParquetInNewTab(String(runtimeFiles.mapa_desagregados?.path || ""))}
                />
                <ArtefactButton
                  available={Boolean(suggestionFilePath)}
                  label="Arquivo de sugestoes"
                  helper="Parquet bruto das sugestoes ativas para auditoria tecnica."
                  onClick={() => openParquetInNewTab(suggestionFilePath)}
                />
              </div>

              <Separator />

              <div className="flex flex-wrap gap-2">
                <Button
                  variant="outline"
                  className="gap-2"
                  onClick={() => {
                    localStorage.setItem(
                      "sefin-audit-open-dir",
                      runtimeFiles.produtos_agregados?.path ? String(runtimeFiles.produtos_agregados.path).replace(/\\/g, "/").replace(/\/[^/]+$/, "") : ""
                    );
                    navigate("/tabelas");
                  }}
                >
                  <Table2 className="h-4 w-4" />
                  Visualizar tabelas
                </Button>
                <Button variant="outline" className="gap-2" onClick={() => openParquetInNewTab(String(runtimeFiles.produtos_agregados?.path || ""))}>
                  <FolderOpen className="h-4 w-4" />
                  Abrir tabela final bruta
                </Button>
                <Button
                  variant="outline"
                  className="gap-2"
                  onClick={() => openParquetInNewTab(String(runtimeFiles.pares_sugeridos_faiss?.path || runtimeFiles.pares_sugeridos_light?.path || ""))}
                >
                  <GitBranch className="h-4 w-4" />
                  Abrir ultimo arquivo de sugestoes
                </Button>
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
