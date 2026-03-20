import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useLocation } from "wouter";
import { Boxes, ChevronLeft, FileSpreadsheet } from "lucide-react";
import { toast } from "sonner";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
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
import { ResumoTab } from "@/components/tratamento-produtos/ResumoTab";
import { SugestoesTab } from "@/components/tratamento-produtos/SugestoesTab";
import { FatoresTab } from "@/components/tratamento-produtos/FatoresTab";
import { AvancadoTab } from "@/components/tratamento-produtos/AvancadoTab";
import type { WorkspaceTab } from "@/components/tratamento-produtos/types";
import { formatCount, normalizeText } from "@/components/tratamento-produtos/formatters";

export default function AnaliseProdutos() {
  const [, navigate] = useLocation();
  const searchParams = new URLSearchParams(window.location.search);
  const cnpj = normalizeText(searchParams.get("cnpj")).replace(/\D/g, "");
  const initialTab = ((): WorkspaceTab => {
    const tab = normalizeText(searchParams.get("tab")).toLowerCase();
    if (tab === "resumo" || tab === "sugestoes" || tab === "fatores" || tab === "avancado") return tab as WorkspaceTab;
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
  const runtimeFiles = runtimeStatus?.files || {};
  const availableRuntimeFiles = Object.values(runtimeFiles).filter((item: any) => item?.exists).length;
  const totalRuntimeFiles = Object.keys(runtimeFiles).length;

  const factorsError = fatoresQuery.error as (Error & { status?: number }) | null;
  const fatoresMissing = fatoresQuery.data ? !fatoresQuery.data.available : factorsError?.status === 404;
  const fatoresIssues = fatoresQuery.data?.issues || [];
  const fatoresCriticos = fatoresIssues.filter((item) => normalizeText(item.severidade).toLowerCase() === "critico").length;
  const fatoresAltos = fatoresIssues.filter((item) => normalizeText(item.severidade).toLowerCase() === "alto").length;

  const faissModeStatus = vectorStatusQuery.data?.status?.modes?.faiss;
  const lightModeStatus = vectorStatusQuery.data?.status?.modes?.light;
  const faissCache = vectorStatusQuery.data?.caches?.faiss as ({ generated_at_utc?: string; stale?: boolean } | undefined);
  const lightCache = vectorStatusQuery.data?.caches?.light as ({ generated_at_utc?: string; stale?: boolean } | undefined);
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

  const openWorkspaceTab = (tab: WorkspaceTab, options?: { agrupamento?: "faiss" | null }) => {
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
        description: `${formatCount(Number(response.total_filtered ?? response.total ?? 0))} visiveis de ${formatCount(Number(response.total_file ?? response.total ?? 0))} no arquivo.`,
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

  const suggestionFilePath = normalizeText(activeSummaryQuery.data?.file_path || "");
  const totalActiveVisible = Number(activeSummaryQuery.data?.total_filtered ?? activeSummaryQuery.data?.total ?? 0);
  const totalActiveFile = Number(activeSummaryQuery.data?.total_file ?? activeSummaryQuery.data?.total ?? 0);
  const modeMessage = suggestionMode === "faiss"
    ? faissModeStatus?.message || "FAISS so roda quando solicitado."
    : lightModeStatus?.message || "Modo leve pronto para uso sob demanda.";

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
          <ResumoTab
            statusResumo={statusResumo}
            gruposRevisaoManual={metaQuery.data?.summary.grupos_revisao_manual || 0}
            runtimeStatusLoaded={Boolean(runtimeStatus)}
            availableRuntimeFiles={availableRuntimeFiles}
            totalRuntimeFiles={totalRuntimeFiles}
            lightAvailable={lightModeStatus?.available !== false}
            faissAvailable={Boolean(faissModeStatus?.available)}
            activeCacheReady={Boolean(activeCache?.generated_at_utc)}
            activeCacheStale={Boolean(activeCache?.stale)}
            fatoresMissing={Boolean(fatoresMissing)}
            fatoresIssuesLength={fatoresIssues.length}
            fatoresCriticos={fatoresCriticos}
            fatoresAltos={fatoresAltos}
            downloadingRevisao={downloadingRevisao}
            runtimeLoading={runtimeLoading}
            onOpenRevisao={openRevisaoFinal}
            onDownloadRevisao={handleDownloadRevisao}
            onRebuildProdutos={handleRebuildProdutos}
            onOpenFatores={() => openWorkspaceTab("fatores")}
            onOpenSugestoes={() => openWorkspaceTab("sugestoes")}
          />
        </TabsContent>

        <TabsContent value="sugestoes" className="space-y-4">
          <SugestoesTab
            suggestionMode={suggestionMode}
            setSuggestionMode={setSuggestionMode}
            suggestionTopK={suggestionTopK}
            setSuggestionTopK={setSuggestionTopK}
            suggestionMinScore={suggestionMinScore}
            setSuggestionMinScore={setSuggestionMinScore}
            suggestionLoading={suggestionLoading}
            faissAvailable={Boolean(faissModeStatus?.available)}
            suggestionFilePath={suggestionFilePath}
            activeSummaryLoading={activeSummaryQuery.isLoading}
            activeVisibleCount={totalActiveVisible}
            activeFileCount={totalActiveFile}
            modeMessage={modeMessage}
            onGenerateSuggestions={handleGenerateSuggestions}
            onOpenSuggestionFile={() => openParquetInNewTab(suggestionFilePath)}
            onClearSuggestions={handleClearSuggestions}
          />
        </TabsContent>

        <TabsContent value="fatores" className="space-y-4">
          <FatoresTab
            fatoresMissing={Boolean(fatoresMissing)}
            fatoresIssuesLength={fatoresIssues.length}
            fatoresCriticos={fatoresCriticos}
            fatoresAltos={fatoresAltos}
            totalRegistros={fatoresQuery.data?.stats.total_registros || 0}
            hasFatoresFile={Boolean(fatoresQuery.data?.file)}
            onOpenRevisaoFatores={openRevisaoFatores}
            onOpenFatoresParquet={() => openParquetInNewTab(fatoresQuery.data?.file || "")}
          />
        </TabsContent>

        <TabsContent value="avancado" className="space-y-4">
          <AvancadoTab
            hasTabelaFinal={Boolean(metaQuery.data?.file_path)}
            hasStatusAnalise={Boolean((runtimeFiles as any).status_analise?.exists)}
            hasBaseDetalhes={Boolean((runtimeFiles as any).base_detalhes?.exists)}
            hasMapaAgregados={Boolean((runtimeFiles as any).mapa_agregados?.exists)}
            hasMapaDesagregados={Boolean((runtimeFiles as any).mapa_desagregados?.exists)}
            hasSuggestionFile={Boolean(suggestionFilePath)}
            onOpenTabelaFinal={() => openParquetInNewTab(metaQuery.data?.file_path || "")}
            onOpenStatusAnalise={() => openParquetInNewTab(String((runtimeFiles as any).status_analise?.path || ""))}
            onOpenBaseDetalhes={() => openParquetInNewTab(String((runtimeFiles as any).base_detalhes?.path || ""))}
            onOpenMapaAgregados={() => openParquetInNewTab(String((runtimeFiles as any).mapa_agregados?.path || ""))}
            onOpenMapaDesagregados={() => openParquetInNewTab(String((runtimeFiles as any).mapa_desagregados?.path || ""))}
            onOpenSuggestionFile={() => openParquetInNewTab(suggestionFilePath)}
            onOpenTabelas={() => {
              localStorage.setItem(
                "sefin-audit-open-dir",
                (runtimeFiles as any).produtos_agregados?.path
                  ? String((runtimeFiles as any).produtos_agregados.path).replace(/\\/g, "/").replace(/\/[^/]+$/, "")
                  : ""
              );
              navigate("/tabelas");
            }}
            onOpenTabelaFinalBruta={() => openParquetInNewTab(String((runtimeFiles as any).produtos_agregados?.path || ""))}
            onOpenUltimoArquivoSugestoes={() => openParquetInNewTab(String((runtimeFiles as any).pares_sugeridos_faiss?.path || (runtimeFiles as any).pares_sugeridos_light?.path || ""))}
          />
        </TabsContent>
      </Tabs>
    </div>
  );
}
