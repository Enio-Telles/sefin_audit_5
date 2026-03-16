import { ReactNode, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import {
  CheckCircle2,
  XCircle,
  AlertCircle,
  Database,
  BarChart3,
  FolderOpen,
  ExternalLink,
  Table2,
  Clock,
  FileText,
  FileSpreadsheet,
  Package,
  Download,
  Loader2,
  Boxes,
  RefreshCw,
  ListTree,
  GitBranch,
  ChevronDown,
} from "lucide-react";
import { useLocation } from "wouter";
import type { AuditPipelineResponse, AuditFileResult, ProdutoAnaliseStatusResumo } from "@/lib/pythonApi";
import {
  clearVectorizacaoCache,
  downloadRevisaoManualExcel,
  getParesGruposSimilares,
  getRuntimeProdutosStatus,
  getStatusAnaliseProdutos,
  getVectorizacaoStatus,
  rebuildRuntimeProdutos,
} from "@/lib/pythonApi";

interface AuditResultViewProps {
  result: AuditPipelineResponse;
  elapsed?: string;
}

function ActionGroup({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="min-w-0 rounded-xl border border-border/70 bg-card/90 p-3 shadow-sm">
      <div className="mb-2 text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">{title}</div>
      <div className="flex flex-wrap gap-2">{children}</div>
    </div>
  );
}

export function AuditResultView({ result, elapsed }: AuditResultViewProps) {
  const [, navigate] = useLocation();
  const cleanCnpj = result.cnpj?.replace(/\D/g, "") || "";
  const [downloadingRevisao, setDownloadingRevisao] = useState(false);
  const [downloadMsg, setDownloadMsg] = useState<string | null>(null);
  const [runtimeLoading, setRuntimeLoading] = useState(false);
  const [suggestionMode, setSuggestionMode] = useState<"off" | "light" | "faiss">("off");
  const [suggestionTopK, setSuggestionTopK] = useState("8");
  const [suggestionMinScore, setSuggestionMinScore] = useState("0.72");
  const [suggestionLoading, setSuggestionLoading] = useState(false);
  const [advancedProductsOpen, setAdvancedProductsOpen] = useState(false);
  const statusQuery = useQuery({
    queryKey: ["audit-produto-status", cleanCnpj],
    queryFn: () => getStatusAnaliseProdutos(cleanCnpj, { includeData: false }),
    enabled: Boolean(cleanCnpj),
    staleTime: 30_000,
    retry: 1,
  });
  const runtimeQuery = useQuery({
    queryKey: ["audit-produto-runtime", cleanCnpj],
    queryFn: () => getRuntimeProdutosStatus(cleanCnpj),
    enabled: Boolean(cleanCnpj),
    staleTime: 30_000,
    retry: 1,
  });
  const vectorStatusQuery = useQuery({
    queryKey: ["audit-produto-vectorizacao", cleanCnpj],
    queryFn: () => getVectorizacaoStatus(cleanCnpj),
    enabled: Boolean(cleanCnpj),
    staleTime: 30_000,
    retry: 1,
  });
  const statusResumo = (statusQuery.data?.resumo || null) as ProdutoAnaliseStatusResumo | null;
  const runtimeStatus = runtimeQuery.data?.runtime || null;
  const faissModeStatus = vectorStatusQuery.data?.status?.modes?.faiss;
  const faissCache = vectorStatusQuery.data?.caches?.faiss;
  const faissCacheTopK = Number(faissCache?.top_k ?? 8);
  const faissCacheMinScore = Number(faissCache?.min_semantic_score ?? 0.62);
  const lightModeStatus = vectorStatusQuery.data?.status?.modes?.light;
  const lightCache = vectorStatusQuery.data?.caches?.light;
  const lightCacheTopK = Number(lightCache?.top_k ?? 8);
  const lightCacheMinScore = Number(lightCache?.min_semantic_score ?? 0.72);
  const faissSummaryQuery = useQuery({
    queryKey: ["audit-produto-faiss-summary", cleanCnpj, faissCache?.generated_at_utc ?? ""],
    queryFn: () =>
      getParesGruposSimilares(cleanCnpj, "faiss", false, {
        topK: Number.isFinite(faissCacheTopK) ? faissCacheTopK : 8,
        minScore: Number.isFinite(faissCacheMinScore) ? faissCacheMinScore : 0.62,
        page: 1,
        pageSize: 1,
        showAnalyzed: false,
      }),
    enabled: Boolean(cleanCnpj && faissCache?.generated_at_utc),
    staleTime: 30_000,
    retry: 1,
  });
  const lightSummaryQuery = useQuery({
    queryKey: ["audit-produto-light-summary", cleanCnpj, lightCache?.generated_at_utc ?? ""],
    queryFn: () =>
      getParesGruposSimilares(cleanCnpj, "light", false, {
        topK: Number.isFinite(lightCacheTopK) ? lightCacheTopK : 8,
        minScore: Number.isFinite(lightCacheMinScore) ? lightCacheMinScore : 0.72,
        page: 1,
        pageSize: 1,
        showAnalyzed: false,
      }),
    enabled: Boolean(cleanCnpj && lightCache?.generated_at_utc),
    staleTime: 30_000,
    retry: 1,
  });
  const faissVisibleCount = Number(faissSummaryQuery.data?.total ?? 0);
  const faissFileCount = Number(faissSummaryQuery.data?.total_file ?? 0);
  const lightVisibleCount = Number(lightSummaryQuery.data?.total ?? 0);
  const lightFileCount = Number(lightSummaryQuery.data?.total_file ?? 0);
  const selectedSuggestionFile =
    suggestionMode === "faiss"
      ? `pares_descricoes_similares_faiss_${cleanCnpj}.parquet`
      : `pares_descricoes_similares_light_${cleanCnpj}.parquet`;
  const selectedSuggestionLabel = suggestionMode === "faiss" ? "FAISS" : suggestionMode === "light" ? "Leve" : "Desligado";
  const activeCache = suggestionMode === "faiss" ? faissCache : suggestionMode === "light" ? lightCache : null;
  const activeSummaryQuery = suggestionMode === "faiss" ? faissSummaryQuery : suggestionMode === "light" ? lightSummaryQuery : null;
  const activeVisibleCount = suggestionMode === "faiss" ? faissVisibleCount : suggestionMode === "light" ? lightVisibleCount : 0;
  const activeFileCount = suggestionMode === "faiss" ? faissFileCount : suggestionMode === "light" ? lightFileCount : 0;
  const visibleProductFiles = (result.arquivos_produtos || []).filter((file) =>
    [
      `produtos_agregados_${cleanCnpj}.parquet`,
      `base_detalhes_produtos_${cleanCnpj}.parquet`,
      `status_analise_produtos_${cleanCnpj}.parquet`,
      `mapa_auditoria_agregados_${cleanCnpj}.parquet`,
      `mapa_auditoria_desagregados_${cleanCnpj}.parquet`,
    ].includes(file.name)
  );

  const handleDownloadRevisao = async () => {
    if (!cleanCnpj) return;
    setDownloadingRevisao(true);
    setDownloadMsg(null);
    try {
      await downloadRevisaoManualExcel(cleanCnpj);
      setDownloadMsg("Download concluido");
    } catch (err: any) {
      setDownloadMsg(err?.message || "Erro ao baixar planilha");
    } finally {
      setDownloadingRevisao(false);
      setTimeout(() => setDownloadMsg(null), 4000);
    }
  };

  const openParquetInNewTab = (filePath: string) => {
    const url = `/tabelas/view?file_path=${encodeURIComponent(filePath)}`;
    window.open(url, "_blank");
  };

  const openAnaliseProdutos = (tab: string = "revisao") => {
    if (!cleanCnpj) return;
    const params = new URLSearchParams({ cnpj: cleanCnpj });
    if (tab) params.set("tab", tab);
    if (tab === "revisao" && suggestionMode === "faiss" && faissCache?.generated_at_utc) {
      params.set("agrupamento", "faiss");
    }
    navigate(`/analise-produtos?${params.toString()}`);
  };

  const handleRebuildProdutos = async () => {
    if (!cleanCnpj) return;
    setRuntimeLoading(true);
    try {
      const res = await rebuildRuntimeProdutos(cleanCnpj);
      setDownloadMsg(`Pipeline de produtos reprocessado (${res.rows} grupos).`);
      await Promise.all([runtimeQuery.refetch(), statusQuery.refetch()]);
    } catch (err: any) {
      setDownloadMsg(err?.message || "Erro ao reprocessar produtos");
    } finally {
      setRuntimeLoading(false);
      setTimeout(() => setDownloadMsg(null), 4000);
    }
  };

  const handleGenerateSuggestions = async () => {
    if (!cleanCnpj || suggestionMode === "off") return;
    setSuggestionLoading(true);
    try {
      const response = await getParesGruposSimilares(cleanCnpj, suggestionMode === "faiss" ? "faiss" : "light", true, {
        topK: Math.max(2, Math.min(20, Number(suggestionTopK) || 8)),
        minScore: Math.max(0.3, Math.min(0.98, Number(suggestionMinScore) || (suggestionMode === "faiss" ? 0.62 : 0.72))),
        page: 1,
        pageSize: 50,
      });
      const totalVisible = Number(response.total_filtered ?? response.total ?? 0);
      const totalFile = Number(response.total_file ?? totalVisible);
      setDownloadMsg(`Sugestoes ${suggestionMode === "faiss" ? "FAISS" : "leves"} geradas (${totalVisible} visiveis de ${totalFile} no arquivo).`);
      await Promise.all([vectorStatusQuery.refetch(), lightSummaryQuery.refetch(), faissSummaryQuery.refetch()]);
    } catch (err: any) {
      setDownloadMsg(err?.message || `Erro ao gerar sugestoes ${suggestionMode === "faiss" ? "FAISS" : "leves"}`);
    } finally {
      setSuggestionLoading(false);
      setTimeout(() => setDownloadMsg(null), 4000);
    }
  };

  const handleClearSuggestions = async () => {
    if (!cleanCnpj || suggestionMode === "off") return;
    setSuggestionLoading(true);
    try {
      await clearVectorizacaoCache(cleanCnpj, suggestionMode === "faiss" ? "faiss" : "light");
      setDownloadMsg(`Cache de sugestoes ${suggestionMode === "faiss" ? "FAISS" : "leves"} removido.`);
      await Promise.all([vectorStatusQuery.refetch(), lightSummaryQuery.refetch(), faissSummaryQuery.refetch()]);
    } catch (err: any) {
      setDownloadMsg(err?.message || "Erro ao limpar cache de sugestoes");
    } finally {
      setSuggestionLoading(false);
      setTimeout(() => setDownloadMsg(null), 4000);
    }
  };

  const openAnaliseFileByName = (expectedName: string) => {
    const allFiles = [...(result.arquivos_analises || []), ...(result.arquivos_produtos || [])];
    const existingFile = allFiles.find((file) => file.name === expectedName);
    const path = existingFile ? existingFile.path : `${result.dir_analises || ""}/${expectedName}`;
    openParquetInNewTab(path.replace(/\\/g, "/"));
  };

  const FileCard = ({ file, index }: { file: AuditFileResult; index: number }) => (
    <div
      className="group flex cursor-pointer items-center gap-3 rounded-xl border border-border/70 bg-card/70 p-3 transition-all duration-200 hover:border-primary/40 hover:bg-accent/50 animate-in fade-in slide-in-from-bottom-2"
      style={{ animationDelay: `${index * 50}ms`, animationFillMode: "backwards" }}
      onClick={() => openParquetInNewTab(file.path)}
      title={`Abrir ${file.name} em nova aba`}
    >
      <FolderOpen className="h-4 w-4 shrink-0 text-emerald-500" />
      <div className="min-w-0 flex-1 space-y-0.5">
        <p className="truncate text-sm font-medium">{file.name}</p>
        <p className="text-xs text-muted-foreground">
          {file.rows} linhas, {file.columns} colunas
          {file.analise && <Badge variant="outline" className="ml-2 border-border/70 bg-background/70 py-0 text-[9px] text-muted-foreground">{file.analise}</Badge>}
        </p>
      </div>
      <ExternalLink className="h-3.5 w-3.5 shrink-0 text-muted-foreground opacity-0 transition-opacity group-hover:opacity-100" />
    </div>
  );

  return (
    <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4">
      <div className="grid grid-cols-1 gap-4 md:grid-cols-3 lg:grid-cols-6">
        <Card className="border-border/70 bg-card/95 shadow-sm">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <div className="rounded-lg bg-blue-500/10 p-2">
                <Database className="h-5 w-5 text-blue-500" />
              </div>
              <div>
                <p className="text-2xl font-bold">{result.arquivos_extraidos?.length || 0}</p>
                <p className="text-xs text-muted-foreground">Consultas</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card className="border-border/70 bg-card/95 shadow-sm">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <div className="rounded-lg bg-orange-500/10 p-2">
                <Package className="h-5 w-5 text-orange-500" />
              </div>
              <div>
                <p className="text-2xl font-bold">{visibleProductFiles.length}</p>
                <p className="text-xs text-muted-foreground">Produtos</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card className="border-border/70 bg-card/95 shadow-sm">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <div className="rounded-lg bg-emerald-500/10 p-2">
                <BarChart3 className="h-5 w-5 text-emerald-500" />
              </div>
              <div>
                <p className="text-2xl font-bold">{result.arquivos_analises?.length || 0}</p>
                <p className="text-xs text-muted-foreground">Analises</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card className="border-border/70 bg-card/95 shadow-sm">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <div className="rounded-lg bg-indigo-500/10 p-2">
                <FileText className="h-5 w-5 text-indigo-500" />
              </div>
              <div>
                <p className="text-2xl font-bold">{result.arquivos_relatorios?.length || 0}</p>
                <p className="text-xs text-muted-foreground">Relatorios</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card className="border-border/70 bg-card/95 shadow-sm">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <div className={`rounded-lg p-2 ${(result.erros?.length || 0) > 0 ? "bg-orange-500/10" : "bg-emerald-500/10"}`}>
                {(result.erros?.length || 0) > 0 ? (
                  <AlertCircle className="h-5 w-5 text-orange-500" />
                ) : (
                  <CheckCircle2 className="h-5 w-5 text-emerald-500" />
                )}
              </div>
              <div>
                <p className="text-2xl font-bold">{result.erros?.length || 0}</p>
                <p className="text-xs text-muted-foreground">Erros</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card className="border-border/70 bg-card/95 shadow-sm">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <div className="rounded-lg bg-violet-500/10 p-2">
                <Clock className="h-5 w-5 text-violet-500" />
              </div>
              <div>
                <p className="text-2xl font-bold">{elapsed || "Concluido"}</p>
                <p className="text-xs text-muted-foreground">Tempo</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <Card className="border-orange-500/20 bg-card/95 shadow-sm">
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-sm font-semibold">
              <Package className="h-4 w-4 text-orange-500" />
              Analise de Produtos
              <Badge variant="outline" className="ml-auto text-xs">
                {visibleProductFiles.length} arquivos
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              <p className="text-sm leading-6 text-muted-foreground">
                A entrada principal da analise de produtos agora fica concentrada em um workspace unico. A revisao final continua disponivel, mas os controles tecnicos ficaram recolhidos para reduzir a carga visual.
              </p>

              <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
                {[
                  { label: "Pendentes", value: statusResumo?.pendentes ?? 0 },
                  { label: "Verificados", value: statusResumo?.verificados ?? 0 },
                  { label: "Consolidados", value: statusResumo?.consolidados ?? 0 },
                  { label: "Decididos", value: (statusResumo?.separados ?? 0) + (statusResumo?.decididos_entre_grupos ?? 0) },
                ].map((item) => (
                  <div key={item.label} className="rounded-xl border border-border/70 bg-accent/35 px-3 py-2">
                    <div className="text-[11px] font-semibold text-muted-foreground">{item.label}</div>
                    <div className="text-lg font-semibold text-foreground">{item.value}</div>
                  </div>
                ))}
              </div>

              <div className="flex flex-wrap gap-2">
                <Button className="gap-2 bg-blue-600 text-white hover:bg-blue-700" onClick={() => openAnaliseProdutos()}>
                  <Boxes className="h-4 w-4" />
                  Abrir analise de produtos
                </Button>
                <Button variant="outline" className="gap-2" onClick={() => openAnaliseProdutos("fatores")}>
                  <FileSpreadsheet className="h-4 w-4" />
                  Ver fatores
                </Button>
                <Button variant="outline" className="gap-2" onClick={handleDownloadRevisao} disabled={downloadingRevisao}>
                  {downloadingRevisao ? <Loader2 className="h-4 w-4 animate-spin" /> : <Download className="h-4 w-4" />}
                  Excel
                </Button>
              </div>

              <Collapsible open={advancedProductsOpen} onOpenChange={setAdvancedProductsOpen}>
                <div className="rounded-xl border border-border/70 bg-accent/25 p-3">
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <div>
                      <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">Opcoes avancadas</div>
                      <div className="mt-1 text-xs text-muted-foreground">
                        Runtime, sugestoes opcionais, artefatos tecnicos e arquivos brutos continuam disponiveis aqui.
                      </div>
                    </div>
                    <CollapsibleTrigger asChild>
                      <Button variant="ghost" size="sm" className="gap-2">
                        {advancedProductsOpen ? "Ocultar detalhes" : "Mostrar detalhes"}
                        <ChevronDown className={`h-4 w-4 transition-transform ${advancedProductsOpen ? "rotate-180" : ""}`} />
                      </Button>
                    </CollapsibleTrigger>
                  </div>

                  <CollapsibleContent className="space-y-3 pt-3">
                    <Separator />
                    <code className="block truncate text-[10px] text-muted-foreground">{result.dir_analises}</code>

                    <div className="rounded-xl border border-border/70 bg-accent/30 px-3 py-3">
                      <div className="mb-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">Runtime de produtos</div>
                      <div className="text-sm text-foreground">
                        {runtimeStatus ? (
                          <>
                            <span>Modo: <strong>fluxo atual</strong>.</span>
                            <span className="ml-2">Somente artefatos do sistema novo sao exibidos.</span>
                          </>
                        ) : (
                          <span>Status nao carregado.</span>
                        )}
                      </div>
                      {runtimeStatus ? (
                        <div className="mt-2 text-xs text-muted-foreground">
                          Artefatos disponiveis: {Object.values(runtimeStatus.files || {}).filter((item) => item.exists).length}/{Object.keys(runtimeStatus.files || {}).length}
                        </div>
                      ) : null}
                      <div className="mt-2 flex flex-wrap gap-2">
                        {runtimeStatus
                          ? Object.entries(runtimeStatus.files || {}).map(([key, item]) => (
                              <span
                                key={key}
                                className={`inline-flex rounded-full px-2 py-1 text-[11px] font-medium ${item.exists ? "border border-emerald-500/30 bg-emerald-500/15 text-emerald-300" : "border border-border/70 bg-background/60 text-muted-foreground"}`}
                              >
                                {key.replaceAll("_", " ")}
                              </span>
                            ))
                          : null}
                      </div>
                      <div className="mt-3 rounded-lg border border-border/70 bg-background/40 px-3 py-2">
                        <div className="flex flex-wrap items-center gap-2 text-xs">
                          <span className="font-semibold text-foreground">Sugestoes opcionais:</span>
                          <Badge variant="outline" className="border-emerald-500/30 bg-emerald-500/10 text-emerald-300">
                            Leve disponivel
                          </Badge>
                          <Badge
                            variant="outline"
                            className={faissModeStatus?.available ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-300" : "border-border/70 bg-background/60 text-muted-foreground"}
                          >
                            {faissModeStatus?.available ? "FAISS disponivel" : "FAISS indisponivel"}
                          </Badge>
                          {activeCache?.generated_at_utc ? (
                            <Badge variant="outline" className={activeCache?.stale ? "border-orange-500/30 bg-orange-500/10 text-orange-300" : "border-blue-500/30 bg-blue-500/10 text-blue-300"}>
                              {activeCache?.stale ? "cache desatualizado" : "cache pronto"}
                            </Badge>
                          ) : (
                            <Badge variant="outline" className="border-border/70 bg-background/60 text-muted-foreground">
                              sem cache
                            </Badge>
                          )}
                        </div>
                        <div className="mt-1 text-xs text-muted-foreground">
                          Modo selecionado: <strong className="text-foreground">{selectedSuggestionLabel}</strong>.{" "}
                          {suggestionMode === "faiss"
                            ? (faissModeStatus?.message || "FAISS desativado ate ser solicitado manualmente.")
                            : (lightModeStatus?.message || "Modo leve desativado ate ser solicitado manualmente.")}
                        </div>
                        {activeCache?.generated_at_utc ? (
                          <div className="mt-2 text-xs text-muted-foreground">
                            {activeSummaryQuery?.isLoading ? (
                              <span>Atualizando contagens de sugestoes...</span>
                            ) : (
                              <span>
                                Visiveis agora: <strong className="text-foreground">{activeVisibleCount}</strong> de{" "}
                                <strong className="text-foreground">{activeFileCount || activeVisibleCount}</strong> no arquivo.
                              </span>
                            )}
                          </div>
                        ) : null}
                        <div className="mt-1 text-[11px] text-muted-foreground">
                          O total visivel exclui pares ocultados por status de analise. O total do arquivo representa o bruto gravado no parquet.
                        </div>
                      </div>
                    </div>

                    <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                      <ActionGroup title="Revisao">
                        <Button
                          variant="outline"
                          size="sm"
                          className="h-8 gap-1.5"
                          onClick={() => {
                            const params = new URLSearchParams({ cnpj: cleanCnpj, tab: "revisao" });
                            if (suggestionMode === "faiss" && faissCache?.generated_at_utc) {
                              params.set("agrupamento", "faiss");
                            }
                            window.open(`/analise-produtos?${params.toString()}`, "_blank");
                          }}
                        >
                          <Boxes className="h-3.5 w-3.5" />
                          Revisao final
                        </Button>
                        <Button
                          variant="outline"
                          size="sm"
                          className="h-8 gap-1.5"
                          onClick={() => {
                            window.open(`/revisao-fatores?cnpj=${cleanCnpj}`, "_blank");
                          }}
                        >
                          <FileSpreadsheet className="h-3.5 w-3.5" />
                          Revisao de fatores
                        </Button>
                      </ActionGroup>

                      <ActionGroup title="Pipeline">
                        <Button
                          variant="outline"
                          size="sm"
                          className="h-8 gap-1.5"
                          onClick={handleRebuildProdutos}
                          disabled={runtimeLoading}
                        >
                          {runtimeLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
                          Reprocessar produtos
                        </Button>
                        <Button
                          variant="outline"
                          size="sm"
                          className="h-8 gap-1.5"
                          onClick={() => void runtimeQuery.refetch()}
                          disabled={runtimeLoading}
                        >
                          <RefreshCw className="h-3.5 w-3.5" />
                          Atualizar runtime
                        </Button>
                      </ActionGroup>

                      <ActionGroup title="Artefatos">
                        <Button
                          variant="outline"
                          size="sm"
                          className="h-8 gap-1.5"
                          onClick={() => {
                            openAnaliseFileByName(`mapa_auditoria_agregados_${cleanCnpj}.parquet`);
                          }}
                        >
                          <ListTree className="h-3.5 w-3.5" />
                          Mapa de agregados
                        </Button>
                        <Button
                          variant="outline"
                          size="sm"
                          className="h-8 gap-1.5"
                          onClick={() => {
                            openAnaliseFileByName(`mapa_auditoria_desagregados_${cleanCnpj}.parquet`);
                          }}
                        >
                          <GitBranch className="h-3.5 w-3.5" />
                          Mapa de desagregados
                        </Button>
                      </ActionGroup>

                      <ActionGroup title="Status">
                        <Button
                          variant="outline"
                          size="sm"
                          className="h-8 gap-1.5"
                          onClick={() => {
                            openAnaliseFileByName(`status_analise_produtos_${cleanCnpj}.parquet`);
                          }}
                        >
                          <BarChart3 className="h-3.5 w-3.5" />
                          Status
                        </Button>
                      </ActionGroup>
                    </div>

                    <ActionGroup title="Sugestoes">
                      <div className="w-full rounded-lg border border-border/70 bg-background/40 p-3">
                        <div className="grid gap-3 md:grid-cols-[minmax(0,1.4fr)_96px_112px]">
                          <div className="min-w-0 space-y-1">
                            <div className="text-[11px] font-medium text-muted-foreground">Modo</div>
                            <Select value={suggestionMode} onValueChange={(value: "off" | "light" | "faiss") => setSuggestionMode(value)}>
                              <SelectTrigger size="sm" className="w-full">
                                <SelectValue placeholder="Selecione" />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="off">Desligado</SelectItem>
                                <SelectItem value="light">Leve</SelectItem>
                                <SelectItem value="faiss">FAISS</SelectItem>
                              </SelectContent>
                            </Select>
                          </div>
                          <div className="space-y-1">
                            <div className="text-[11px] font-medium text-muted-foreground">Top K</div>
                            <Input
                              className="h-8 bg-background/60 text-xs"
                              inputMode="numeric"
                              value={suggestionTopK}
                              onChange={(event) => setSuggestionTopK(event.target.value)}
                            />
                          </div>
                          <div className="space-y-1">
                            <div className="text-[11px] font-medium text-muted-foreground">Score min.</div>
                            <Input
                              className="h-8 bg-background/60 text-xs"
                              inputMode="decimal"
                              value={suggestionMinScore}
                              onChange={(event) => setSuggestionMinScore(event.target.value)}
                            />
                          </div>
                        </div>
                        <div className="mt-3 grid gap-3 md:grid-cols-3">
                          <Button
                            variant="outline"
                            size="sm"
                            className="h-8 w-full gap-1.5"
                            onClick={handleGenerateSuggestions}
                            disabled={suggestionLoading || suggestionMode === "off" || (suggestionMode === "faiss" && !faissModeStatus?.available)}
                          >
                            {suggestionLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
                            Gerar sugestoes
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            className="h-8 w-full gap-1.5"
                            onClick={() => openAnaliseFileByName(selectedSuggestionFile)}
                            disabled={suggestionMode === "off"}
                          >
                            <ExternalLink className="h-3.5 w-3.5" />
                            Abrir arquivo
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-8 w-full gap-1.5"
                            onClick={handleClearSuggestions}
                            disabled={suggestionLoading || suggestionMode === "off"}
                          >
                            <XCircle className="h-3.5 w-3.5" />
                            Limpar cache
                          </Button>
                        </div>
                      </div>
                    </ActionGroup>

                    <div className="flex items-start justify-start">
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-8 gap-1.5"
                        onClick={() => {
                          localStorage.setItem("sefin-audit-open-dir", result.dir_analises || "");
                          navigate("/tabelas");
                        }}
                      >
                        <Table2 className="h-3.5 w-3.5" />
                        Arquivos
                      </Button>
                    </div>

                    <div className="rounded-xl border border-border/70 bg-background/40 p-3">
                      <div className="mb-2 text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">Arquivos do runtime</div>
                      <ScrollArea className="h-[220px]">
                        <div className="space-y-1.5">
                          {visibleProductFiles.length > 0 ? (
                            visibleProductFiles.map((file, i) => <FileCard key={file.path} file={file} index={i} />)
                          ) : (
                            <div className="flex items-center justify-center py-8 text-xs text-muted-foreground">
                              Nenhum arquivo de produtos gerado
                            </div>
                          )}
                        </div>
                      </ScrollArea>
                    </div>
                  </CollapsibleContent>
                </div>
              </Collapsible>

              {downloadMsg && <p className="text-xs text-muted-foreground">{downloadMsg}</p>}
            </div>
          </CardContent>
        </Card>

        <Card className="border-border/70 bg-card/95 shadow-sm">
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-sm font-semibold">
              <Database className="h-4 w-4 text-blue-500" />
              Dados Extraidos
              <Badge variant="outline" className="ml-auto text-xs">
                {(result.arquivos_extraidos?.length || 0)} arquivos
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent>
            <ScrollArea className="h-[350px]">
              <div className="space-y-1.5">
                {result.arquivos_extraidos?.map((file, i) => <FileCard key={file.path} file={file} index={i} />)}
              </div>
            </ScrollArea>
            <Separator className="my-3" />
            <div className="flex items-center justify-between">
              <code className="max-w-[250px] truncate text-[10px] text-muted-foreground">{result.dir_parquet}</code>
              <Button
                variant="outline"
                size="sm"
                className="h-7 gap-1.5 text-xs"
                onClick={() => {
                  localStorage.setItem("sefin-audit-open-dir", result.dir_parquet);
                  navigate("/tabelas");
                }}
              >
                <Table2 className="h-3 w-3" />
                Ver pasta
              </Button>
            </div>
          </CardContent>
        </Card>

        <Card className="border-border/70 bg-card/95 shadow-sm">
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-sm font-semibold">
              <BarChart3 className="h-4 w-4 text-emerald-500" />
              Relatorios e Analises
              <Badge variant="outline" className="ml-auto text-xs">
                {(result.arquivos_analises?.length || 0)} arquivos
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent>
            {result.etapas?.find((e) => e.etapa === "Análises")?.analises && (
              <div className="mb-4 space-y-2">
                {result.etapas.find((e) => e.etapa === "Análises")!.analises!.map((a, i) => (
                  <div key={i} className="flex items-center gap-2 rounded-lg bg-muted/30 p-2">
                    {a.status === "sucesso" ? (
                      <CheckCircle2 className="h-4 w-4 text-emerald-500" />
                    ) : a.status === "erro" ? (
                      <XCircle className="h-4 w-4 text-red-500" />
                    ) : (
                      <AlertCircle className="h-4 w-4 text-yellow-500" />
                    )}
                    <span className="text-sm font-medium">{a.nome}</span>
                    <Badge
                      variant={a.status === "sucesso" ? "default" : a.status === "erro" ? "destructive" : "secondary"}
                      className="ml-auto text-[10px]"
                    >
                      {a.status}
                    </Badge>
                  </div>
                ))}
              </div>
            )}

            <ScrollArea className="h-[200px]">
              <div className="space-y-1.5">
                {(result.arquivos_analises?.length || 0) > 0 ? (
                  result.arquivos_analises.map((file, i) => <FileCard key={file.path} file={file} index={i} />)
                ) : (
                  <div className="flex items-center justify-center py-8 text-xs text-muted-foreground">
                    Nenhum arquivo de analise gerado
                  </div>
                )}
              </div>
            </ScrollArea>

            {(result.arquivos_analises?.length || 0) > 0 && (
              <>
                <Separator className="my-3" />
                <div className="flex items-center justify-between">
                  <code className="max-w-[250px] truncate text-[10px] text-muted-foreground">{result.dir_analises}</code>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 gap-1.5 text-xs"
                    onClick={() => {
                      localStorage.setItem("sefin-audit-open-dir", result.dir_analises);
                      navigate("/tabelas");
                    }}
                  >
                    <Table2 className="h-3 w-3" />
                    Ver analises
                  </Button>
                </div>
              </>
            )}
          </CardContent>
        </Card>
      </div>

      {(result.arquivos_relatorios?.length || 0) > 0 && (
        <Card className="border-indigo-500/20 bg-card/95 shadow-sm">
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-sm font-semibold">
              <FileText className="h-4 w-4 text-indigo-500" />
              Documentos Gerados
              <Badge variant="outline" className="ml-auto text-xs">
                {(result.arquivos_relatorios?.length || 0)} documentos
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {result.arquivos_relatorios?.map((doc, i) => (
                <div
                  key={doc.path}
                  className="group flex items-center gap-3 rounded-lg border bg-muted/30 p-3 animate-in fade-in slide-in-from-bottom-2"
                  style={{ animationDelay: `${i * 80}ms`, animationFillMode: "backwards" }}
                >
                  <div className={`rounded-md p-1.5 ${(doc.tipo?.includes("Word") || false) ? "bg-blue-500/10" : "bg-gray-500/10"}`}>
                    <FileText className={`h-4 w-4 ${(doc.tipo?.includes("Word") || false) ? "text-blue-500" : "text-gray-500"}`} />
                  </div>
                  <div className="min-w-0 flex-1">
                    <p className="truncate text-sm font-medium">{doc.name}</p>
                    <p className="text-xs text-muted-foreground">{doc.tipo || "Desconhecido"}</p>
                  </div>
                  <Badge variant="outline" className="shrink-0 py-0 text-[9px]">
                    {(doc.tipo?.includes("Word") || false) ? "DOCX" : "TXT"}
                  </Badge>
                </div>
              ))}
            </div>
            <Separator className="my-3" />
            <div className="flex items-center justify-between">
              <code className="max-w-[350px] truncate text-[10px] text-muted-foreground">{result.dir_relatorios}</code>
              <Button
                variant="outline"
                size="sm"
                className="h-7 gap-1.5 text-xs"
                onClick={() => {
                  localStorage.setItem("sefin-audit-open-dir", result.dir_relatorios);
                  navigate("/tabelas");
                }}
              >
                <FolderOpen className="h-3 w-3" />
                Ver relatorios
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {(result.erros?.length || 0) > 0 && (
        <Card className="border-orange-500/30">
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-sm font-semibold text-orange-500">
              <AlertCircle className="h-4 w-4" />
              Erros ({result.erros?.length || 0})
            </CardTitle>
          </CardHeader>
          <CardContent>
            <ScrollArea className="h-[120px]">
              <div className="space-y-1 font-mono text-xs">
                {result.erros?.map((erro, i) => (
                  <p key={i} className="text-orange-600 dark:text-orange-400">{erro}</p>
                ))}
              </div>
            </ScrollArea>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
