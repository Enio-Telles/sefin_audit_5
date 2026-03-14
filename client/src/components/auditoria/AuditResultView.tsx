import { ReactNode, useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Button } from "@/components/ui/button";
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
  Package,
  Download,
  Loader2,
  Boxes,
  MousePointerClick,
  RefreshCw,
  ListTree,
  GitBranch,
  GitMerge,
} from "lucide-react";
import { useLocation } from "wouter";
import type { AuditPipelineResponse, AuditFileResult, ProdutoAnaliseStatusResumo } from "@/lib/pythonApi";
import { clearVectorizacaoCache, downloadRevisaoManualExcel, getParesGruposSimilares, getStatusAnaliseProdutos, getVectorizacaoStatus } from "@/lib/pythonApi";

interface AuditResultViewProps {
  result: AuditPipelineResponse;
  elapsed?: string;
}

function ActionGroup({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="rounded-lg border bg-slate-50/80 p-3">
      <div className="mb-2 text-[11px] font-semibold text-slate-600">{title}</div>
      <div className="flex flex-wrap gap-2">{children}</div>
    </div>
  );
}

export function AuditResultView({ result, elapsed }: AuditResultViewProps) {
  const [, navigate] = useLocation();
  const [downloadingRevisao, setDownloadingRevisao] = useState(false);
  const [downloadMsg, setDownloadMsg] = useState<string | null>(null);
  const [statusResumo, setStatusResumo] = useState<ProdutoAnaliseStatusResumo | null>(null);
  const [vectorStatus, setVectorStatus] = useState<{
    available: boolean;
    message: string;
    model_name?: string;
    engine?: string | null;
  } | null>(null);
  const [vectorCaches, setVectorCaches] = useState<{ semantic?: Record<string, unknown>; hybrid?: Record<string, unknown> } | null>(null);
  const [currentBaseHash, setCurrentBaseHash] = useState<string | null>(null);
  const [vectorLoading, setVectorLoading] = useState(false);
  const vectorEngine = String(vectorStatus?.engine || "").toUpperCase();
  const vectorEngineIsFaiss = vectorEngine === "FAISS";
  const vectorEngineIsNumpy = vectorEngine === "NUMPY";
  const vectorCacheStale = Boolean(vectorCaches?.semantic?.stale || vectorCaches?.hybrid?.stale);
  const vectorRecommendation = !vectorStatus?.available
    ? "Vetorizacao indisponivel"
    : vectorCacheStale
      ? "Recalculo recomendado"
      : vectorEngineIsFaiss
        ? "Usar FAISS"
        : vectorEngineIsNumpy
          ? "Fallback aceitavel"
          : "Status normal";

  useEffect(() => {
    let active = true;
    const cleanCnpj = result.cnpj?.replace(/\D/g, "");
    if (!cleanCnpj) {
      setStatusResumo(null);
      return;
    }

    getStatusAnaliseProdutos(cleanCnpj)
      .then((res) => {
        if (active) {
          setStatusResumo(res.resumo || null);
        }
      })
      .catch(() => {
        if (active) {
          setStatusResumo(null);
        }
      });

    getVectorizacaoStatus(cleanCnpj)
      .then((res) => {
        if (active) {
          setVectorStatus(res.status || null);
          setVectorCaches(res.caches || null);
          setCurrentBaseHash(res.current_base_hash || null);
        }
      })
      .catch(() => {
        if (active) {
          setVectorStatus(null);
          setVectorCaches(null);
          setCurrentBaseHash(null);
        }
      });

    return () => {
      active = false;
    };
  }, [result.cnpj]);

  const handleDownloadRevisao = async () => {
    if (!result.cnpj) return;
    const cleanCnpj = result.cnpj.replace(/\D/g, "");
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

  const refreshVectorStatus = async () => {
    if (!result.cnpj) return;
    const cleanCnpj = result.cnpj.replace(/\D/g, "");
    try {
      const res = await getVectorizacaoStatus(cleanCnpj);
      setVectorStatus(res.status || null);
      setVectorCaches(res.caches || null);
      setCurrentBaseHash(res.current_base_hash || null);
    } catch {
      setVectorStatus(null);
      setVectorCaches(null);
      setCurrentBaseHash(null);
    }
  };

  const handleRecalculateSemantic = async () => {
    if (!result.cnpj) return;
    const cleanCnpj = result.cnpj.replace(/\D/g, "");
    setVectorLoading(true);
    try {
      const res = await getParesGruposSimilares(cleanCnpj, "semantic", true);
      if (!res.success) {
        setDownloadMsg(res.message || "Modo semantico indisponivel.");
      } else {
        setDownloadMsg("Pares semanticos recalculados");
      }
      await refreshVectorStatus();
    } catch (err: any) {
      setDownloadMsg(err?.message || "Erro ao recalcular pares semanticos");
    } finally {
      setVectorLoading(false);
      setTimeout(() => setDownloadMsg(null), 4000);
    }
  };

  const handleRecalculateHybrid = async () => {
    if (!result.cnpj) return;
    const cleanCnpj = result.cnpj.replace(/\D/g, "");
    setVectorLoading(true);
    try {
      const res = await getParesGruposSimilares(cleanCnpj, "hybrid", true);
      if (!res.success) {
        setDownloadMsg(res.message || "Modo hibrido indisponivel.");
      } else {
        setDownloadMsg("Pares hibridos recalculados");
      }
      await refreshVectorStatus();
    } catch (err: any) {
      setDownloadMsg(err?.message || "Erro ao recalcular pares hibridos");
    } finally {
      setVectorLoading(false);
      setTimeout(() => setDownloadMsg(null), 4000);
    }
  };

  const handleClearVectorCache = async () => {
    if (!result.cnpj) return;
    const cleanCnpj = result.cnpj.replace(/\D/g, "");
    setVectorLoading(true);
    try {
      await clearVectorizacaoCache(cleanCnpj, "all");
      setDownloadMsg("Cache vetorizado removido");
      await refreshVectorStatus();
    } catch (err: any) {
      setDownloadMsg(err?.message || "Erro ao limpar cache vetorizado");
    } finally {
      setVectorLoading(false);
      setTimeout(() => setDownloadMsg(null), 4000);
    }
  };

  const openAnaliseFileByName = (expectedName: string) => {
    const allFiles = [...(result.arquivos_analises || []), ...(result.arquivos_produtos || [])];
    const existingFile = allFiles.find((file) => file.name === expectedName);
    const path = existingFile ? existingFile.path : `${result.dir_analises || ""}/${expectedName}`;
    openParquetInNewTab(path.replace(/\\/g, "/"));
  };

  const openVectorAnaliseFile = (expectedName: string, stale?: boolean) => {
    if (stale && !window.confirm("O cache vetorizado desta tabela esta desatualizado em relacao a base atual. Deseja abrir mesmo assim?")) {
      return;
    }
    openAnaliseFileByName(expectedName);
  };

  const FileCard = ({ file, index }: { file: AuditFileResult; index: number }) => (
    <div
      className="group flex cursor-pointer items-center gap-3 rounded-lg border bg-muted/30 p-3 transition-all duration-200 hover:border-primary/30 hover:bg-primary/5 animate-in fade-in slide-in-from-bottom-2"
      style={{ animationDelay: `${index * 50}ms`, animationFillMode: "backwards" }}
      onClick={() => openParquetInNewTab(file.path)}
      title={`Abrir ${file.name} em nova aba`}
    >
      <FolderOpen className="h-4 w-4 shrink-0 text-emerald-500" />
      <div className="min-w-0 flex-1 space-y-0.5">
        <p className="truncate text-sm font-medium">{file.name}</p>
        <p className="text-xs text-muted-foreground">
          {file.rows} linhas, {file.columns} colunas
          {file.analise && <Badge variant="outline" className="ml-2 py-0 text-[9px]">{file.analise}</Badge>}
        </p>
      </div>
      <ExternalLink className="h-3.5 w-3.5 shrink-0 text-muted-foreground opacity-0 transition-opacity group-hover:opacity-100" />
    </div>
  );

  return (
    <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4">
      <div className="grid grid-cols-1 gap-4 md:grid-cols-3 lg:grid-cols-6">
        <Card>
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
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <div className="rounded-lg bg-orange-500/10 p-2">
                <Package className="h-5 w-5 text-orange-500" />
              </div>
              <div>
                <p className="text-2xl font-bold">{result.arquivos_produtos?.length || 0}</p>
                <p className="text-xs text-muted-foreground">Produtos</p>
              </div>
            </div>
          </CardContent>
        </Card>
        <Card>
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
        <Card>
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
        <Card>
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
        <Card>
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
        <Card className="border-orange-500/20">
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-sm font-semibold">
              <Package className="h-4 w-4 text-orange-500" />
              Unificacao de Produtos
              <Badge variant="outline" className="ml-auto text-xs">
                {(result.arquivos_produtos?.length || 0)} arquivos
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent>
            <ScrollArea className="h-[350px]">
              <div className="space-y-1.5">
                {(result.arquivos_produtos?.length || 0) > 0 ? (
                  result.arquivos_produtos.map((file, i) => <FileCard key={file.path} file={file} index={i} />)
                ) : (
                  <div className="flex items-center justify-center py-8 text-xs text-muted-foreground">
                    Nenhum arquivo de produtos gerado
                  </div>
                )}
              </div>
            </ScrollArea>
            <Separator className="my-3" />
            <div className="space-y-3">
              <code className="block truncate text-[10px] text-muted-foreground">{result.dir_analises}</code>

              <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-5">
                {[
                  { label: "Pendentes", value: statusResumo?.pendentes ?? 0 },
                  { label: "Verificados", value: statusResumo?.verificados ?? 0 },
                  { label: "Consolidados", value: statusResumo?.consolidados ?? 0 },
                  { label: "Separados", value: statusResumo?.separados ?? 0 },
                  { label: "Decididos entre grupos", value: statusResumo?.decididos_entre_grupos ?? 0 },
                ].map((item) => (
                  <div key={item.label} className="rounded-lg border bg-white px-3 py-2">
                    <div className="text-[11px] font-semibold text-slate-500">{item.label}</div>
                    <div className="text-lg font-semibold text-slate-900">{item.value}</div>
                  </div>
                ))}
              </div>

              <div
                className={`rounded-lg border px-3 py-3 ${
                  vectorCaches?.semantic?.stale || vectorCaches?.hybrid?.stale
                    ? "border-amber-300 bg-amber-50"
                    : "bg-white"
                }`}
              >
                <div className="mb-1 text-[11px] font-semibold text-slate-500">Vetorizacao</div>
                <div className="text-sm text-slate-700">
                  {vectorStatus ? (
                    <>
                      <span>Status: <strong>{vectorStatus.available ? "disponivel" : "indisponivel"}</strong>.</span>
                      {vectorStatus.engine ? <span className="ml-2">Engine: <strong>{vectorEngine}</strong>.</span> : null}
                      {vectorStatus.model_name ? <span className="ml-2">Modelo: <strong>{vectorStatus.model_name}</strong>.</span> : null}
                    </>
                  ) : (
                    <span>Status nao carregado.</span>
                  )}
                </div>
                {vectorEngineIsFaiss ? (
                  <div
                    className="mt-2 inline-flex rounded-full bg-emerald-100 px-2 py-1 text-xs font-semibold text-emerald-700"
                    title="FAISS usa indice vetorial dedicado e tende a ser mais rapido para busca de vizinhos."
                  >
                    FAISS ativo
                  </div>
                ) : null}
                {vectorEngineIsNumpy ? (
                  <div
                    className="mt-2 inline-flex rounded-full bg-amber-100 px-2 py-1 text-xs font-semibold text-amber-800"
                    title="NUMPY fallback dispensa FAISS, mas tende a ser mais lento em bases maiores."
                  >
                    NUMPY fallback
                  </div>
                ) : null}
                {vectorEngineIsFaiss ? (
                  <div className="mt-1 text-xs text-emerald-700">
                    Busca vetorial otimizada, mais adequada para uso recorrente.
                  </div>
                ) : null}
                {vectorEngineIsNumpy ? (
                  <div className="mt-1 text-xs text-amber-800">
                    Fallback sem FAISS. Funciona, mas com custo maior em CPU para bases maiores.
                  </div>
                ) : null}
                {vectorStatus?.message ? <div className="mt-1 text-xs text-slate-500">{vectorStatus.message}</div> : null}
                <div
                  className={`mt-1 inline-flex rounded-full px-2 py-1 text-xs font-semibold ${
                    !vectorStatus?.available
                      ? "bg-slate-200 text-slate-700"
                      : vectorCacheStale
                        ? "bg-amber-100 text-amber-800"
                        : vectorEngineIsFaiss
                          ? "bg-emerald-100 text-emerald-700"
                          : "bg-sky-100 text-sky-700"
                  }`}
                >
                  {vectorRecommendation}
                </div>
                {currentBaseHash ? <div className="mt-1 text-xs text-slate-500">Base atual: {currentBaseHash.slice(0, 10)}...</div> : null}
                <div className="mt-1 text-xs text-slate-500">
                  {vectorCaches?.semantic?.generated_at_utc ? (
                    <span>Semantico: {new Date(String(vectorCaches.semantic.generated_at_utc)).toLocaleString("pt-BR")}{vectorCaches.semantic?.stale ? " (desatualizado)" : ""}.</span>
                  ) : (
                    <span>Semantico sem cache.</span>
                  )}
                  {" "}
                  {vectorCaches?.hybrid?.generated_at_utc ? (
                    <span>Hibrido: {new Date(String(vectorCaches.hybrid.generated_at_utc)).toLocaleString("pt-BR")}{vectorCaches.hybrid?.stale ? " (desatualizado)" : ""}.</span>
                  ) : (
                    <span>Hibrido sem cache.</span>
                  )}
                </div>
                {(vectorCaches?.semantic?.stale || vectorCaches?.hybrid?.stale) ? (
                  <div className="mt-2 text-xs font-medium text-amber-800">
                    Ha cache vetorizado desatualizado. Recalcule antes de usar as tabelas semanticas/hibridas.
                  </div>
                ) : null}
                <div className="mt-1 text-xs text-slate-500">
                  {vectorCaches?.semantic?.top_k ? <span>Top K semantico: {String(vectorCaches.semantic.top_k)}.</span> : null}
                  {" "}
                  {vectorCaches?.semantic?.min_semantic_score != null ? <span>Limiar semantico: {String(vectorCaches.semantic.min_semantic_score)}.</span> : null}
                  {" "}
                  {vectorCaches?.hybrid?.top_k ? <span>Top K hibrido: {String(vectorCaches.hybrid.top_k)}.</span> : null}
                  {" "}
                  {vectorCaches?.hybrid?.min_semantic_score != null ? <span>Limiar hibrido: {String(vectorCaches.hybrid.min_semantic_score)}.</span> : null}
                </div>
              </div>

              <div className="grid gap-3 xl:grid-cols-[1.2fr_1fr_1.4fr_0.8fr_auto]">
                <ActionGroup title="Revisao">
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-8 gap-1.5"
                    onClick={handleDownloadRevisao}
                    disabled={downloadingRevisao}
                    title="Baixar planilha de revisao residual"
                  >
                    {downloadingRevisao ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Download className="h-3.5 w-3.5" />}
                    Excel
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-8 gap-1.5"
                    onClick={() => {
                      const cleanCnpj = result.cnpj.replace(/\D/g, "");
                      window.open(`/revisao-manual?cnpj=${cleanCnpj}`, "_blank");
                    }}
                  >
                    <Boxes className="h-3.5 w-3.5" />
                    Revisao residual
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-8 gap-1.5"
                    onClick={() => {
                      const cleanCnpj = result.cnpj.replace(/\D/g, "");
                      const expectedFileName = `produtos_agregados_${cleanCnpj}.parquet`;
                      const productFile = result.arquivos_produtos?.find((f) => f.name === expectedFileName);
                      const path = productFile ? productFile.path : `${result.dir_analises || ""}/${expectedFileName}`;
                      const normalizedPath = path.replace(/\\/g, "/");
                      window.open(
                        `/agregacao-selecao?cnpj=${cleanCnpj}&file_path=${encodeURIComponent(normalizedPath)}`,
                        "_blank"
                      );
                    }}
                  >
                    <MousePointerClick className="h-3.5 w-3.5" />
                    Consolidacao por selecao
                  </Button>
                </ActionGroup>

                <ActionGroup title="Mapas">
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-8 gap-1.5"
                    onClick={() => {
                      const cleanCnpj = result.cnpj.replace(/\D/g, "");
                      openAnaliseFileByName(`mapa_auditoria_agregados_${cleanCnpj}.parquet`);
                    }}
                  >
                    <ListTree className="h-3.5 w-3.5" />
                    Consolidacoes
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-8 gap-1.5"
                    onClick={() => {
                      const cleanCnpj = result.cnpj.replace(/\D/g, "");
                      openAnaliseFileByName(`mapa_auditoria_desagregados_${cleanCnpj}.parquet`);
                    }}
                  >
                    <GitBranch className="h-3.5 w-3.5" />
                    Separacoes
                  </Button>
                </ActionGroup>

                <ActionGroup title="Descricoes">
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-8 gap-1.5"
                    onClick={() => {
                      const cleanCnpj = result.cnpj.replace(/\D/g, "");
                      openAnaliseFileByName(`mapa_auditoria_descricoes_aplicadas_${cleanCnpj}.parquet`);
                    }}
                  >
                    <CheckCircle2 className="h-3.5 w-3.5" />
                    Aplicadas
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-8 gap-1.5"
                    onClick={() => {
                      const cleanCnpj = result.cnpj.replace(/\D/g, "");
                      openAnaliseFileByName(`mapa_auditoria_descricoes_${cleanCnpj}.parquet`);
                    }}
                  >
                    <FileText className="h-3.5 w-3.5" />
                    Auditoria
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-8 gap-1.5"
                    onClick={() => {
                      const cleanCnpj = result.cnpj.replace(/\D/g, "");
                      openAnaliseFileByName(`mapa_auditoria_descricoes_bloqueadas_${cleanCnpj}.parquet`);
                    }}
                  >
                    <AlertCircle className="h-3.5 w-3.5" />
                    Bloqueadas
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-8 gap-1.5"
                    onClick={() => {
                      const cleanCnpj = result.cnpj.replace(/\D/g, "");
                      openAnaliseFileByName(`mapa_manual_descricoes_${cleanCnpj}.parquet`);
                    }}
                  >
                    <GitMerge className="h-3.5 w-3.5" />
                    Manual
                  </Button>
                </ActionGroup>

                <ActionGroup title="Status">
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-8 gap-1.5"
                    onClick={() => {
                      const cleanCnpj = result.cnpj.replace(/\D/g, "");
                      openAnaliseFileByName(`status_analise_produtos_${cleanCnpj}.parquet`);
                    }}
                  >
                    <BarChart3 className="h-3.5 w-3.5" />
                    Status
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-8 gap-1.5"
                    onClick={() => {
                      const cleanCnpj = result.cnpj.replace(/\D/g, "");
                      openAnaliseFileByName(`pares_descricoes_similares_${cleanCnpj}.parquet`);
                    }}
                  >
                    <GitMerge className="h-3.5 w-3.5" />
                    Pares
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-8 gap-1.5"
                    onClick={() => {
                      const cleanCnpj = result.cnpj.replace(/\D/g, "");
                      openVectorAnaliseFile(`pares_descricoes_similares_semanticos_${cleanCnpj}.parquet`, Boolean(vectorCaches?.semantic?.stale));
                    }}
                  >
                    <GitMerge className="h-3.5 w-3.5" />
                    {`Pares semanticos${vectorCaches?.semantic?.stale ? " (desatualizado)" : ""}`}
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-8 gap-1.5"
                    onClick={() => {
                      const cleanCnpj = result.cnpj.replace(/\D/g, "");
                      openVectorAnaliseFile(`pares_descricoes_similares_hibridos_${cleanCnpj}.parquet`, Boolean(vectorCaches?.hybrid?.stale));
                    }}
                  >
                    <GitMerge className="h-3.5 w-3.5" />
                    {`Pares hibridos${vectorCaches?.hybrid?.stale ? " (desatualizado)" : ""}`}
                  </Button>
                  <Button variant="outline" size="sm" className="h-8 gap-1.5" onClick={handleRecalculateSemantic} disabled={vectorLoading}>
                    {vectorLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
                    Recalcular semanticos
                  </Button>
                  <Button variant="outline" size="sm" className="h-8 gap-1.5" onClick={handleRecalculateHybrid} disabled={vectorLoading}>
                    {vectorLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
                    Recalcular hibridos
                  </Button>
                  <Button variant="outline" size="sm" className="h-8 gap-1.5" onClick={handleClearVectorCache} disabled={vectorLoading}>
                    <XCircle className="h-3.5 w-3.5" />
                    Limpar cache
                  </Button>
                </ActionGroup>

                <div className="flex items-start justify-start lg:justify-end">
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
              </div>

              {downloadMsg && <p className="text-xs text-muted-foreground">{downloadMsg}</p>}
            </div>
          </CardContent>
        </Card>

        <Card>
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

        <Card>
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
        <Card className="border-indigo-500/20">
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
