import { Boxes, ChevronLeft, FileSpreadsheet } from "lucide-react";
import { useLocation } from "wouter";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { RevisaoFinalProdutosView } from "@/pages/RevisaoFinalProdutos";
import { ResumoTab } from "@/components/tratamento-produtos/ResumoTab";
import { SugestoesTab } from "@/components/tratamento-produtos/SugestoesTab";
import { FatoresTab } from "@/components/tratamento-produtos/FatoresTab";
import { AvancadoTab } from "@/components/tratamento-produtos/AvancadoTab";
import type { WorkspaceTab } from "@/components/tratamento-produtos/types";
import { formatCount, normalizeText } from "@/components/tratamento-produtos/formatters";
import { useProdutoWorkspace } from "@/hooks/useProdutoWorkspace";

export default function AnaliseProdutos() {
  const [, navigate] = useLocation();
  const searchParams = new URLSearchParams(window.location.search);
  const cnpj = normalizeText(searchParams.get("cnpj")).replace(/\D/g, "");

  const {
    activeTab,
    statusResumo,
    runtimeStatus,
    runtimeFiles,
    availableRuntimeFiles,
    totalRuntimeFiles,
    fatoresMissing,
    fatoresIssues,
    fatoresCriticos,
    fatoresAltos,
    faissModeStatus,
    lightModeStatus,
    activeCache,
    metaQuery,
    fatoresQuery,
    activeSummaryQuery,
    suggestionMode,
    setSuggestionMode,
    suggestionTopK,
    setSuggestionTopK,
    suggestionMinScore,
    setSuggestionMinScore,
    suggestionLoading,
    suggestionFilePath,
    totalActiveVisible,
    totalActiveFile,
    modeMessage,
    downloadingRevisao,
    runtimeLoading,
    openParquetInNewTab,
    openWorkspaceTab,
    openRevisaoFinal,
    openRevisaoFatores,
    handleDownloadRevisao,
    handleRebuildProdutos,
    handleGenerateSuggestions,
    handleClearSuggestions,
  } = useProdutoWorkspace(cnpj, navigate);

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
