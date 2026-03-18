import { ChangeEvent, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useLocation } from "wouter";
import {
  AlertTriangle,
  ChevronLeft,
  FileSpreadsheet,
  FolderOpen,
  Loader2,
  RefreshCw,
  Search,
  ShieldAlert,
  Upload,
  Wrench,
} from "lucide-react";
import { toast } from "sonner";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  calcularFatoresConversao,
  diagnosticarFatoresConversao,
  importFatoresExcel,
  type FatorDiagnosticoItem,
} from "@/lib/pythonApi";

type SeverityFilter = "todos" | "critico" | "alto" | "medio" | "baixo";
type ActiveTab = "ocorrencias" | "resumo";

const SEVERITY_META: Record<
  Exclude<SeverityFilter, "todos">,
  {
    label: string;
    badgeClass: string;
    chipClass: string;
    rowClass: string;
  }
> = {
  critico: {
    label: "Critico",
    badgeClass: "border-red-200 bg-red-50 text-red-700",
    chipClass: "border-red-300 bg-red-600 text-white",
    rowClass: "bg-red-50/55 hover:bg-red-50",
  },
  alto: {
    label: "Alto",
    badgeClass: "border-amber-200 bg-amber-50 text-amber-800",
    chipClass: "border-amber-300 bg-amber-600 text-white",
    rowClass: "bg-amber-50/45 hover:bg-amber-50",
  },
  medio: {
    label: "Medio",
    badgeClass: "border-sky-200 bg-sky-50 text-sky-700",
    chipClass: "border-sky-300 bg-sky-600 text-white",
    rowClass: "bg-sky-50/35 hover:bg-sky-50",
  },
  baixo: {
    label: "Baixo",
    badgeClass: "border-slate-200 bg-slate-100 text-slate-700",
    chipClass: "border-slate-300 bg-slate-700 text-white",
    rowClass: "hover:bg-slate-50/80",
  },
};

const ISSUE_TYPE_META: Record<string, { label: string; helper: string }> = {
  FATOR_INVALIDO: {
    label: "Fator invalido",
    helper: "Fatores nulos, zero ou negativos precisam de revisao imediata.",
  },
  FATOR_EXTREMO_ALTO: {
    label: "Fator extremo alto",
    helper: "Pode indicar erro de unidade, embalagem ou digitacao.",
  },
  FATOR_EXTREMO_BAIXO: {
    label: "Fator extremo baixo",
    helper: "Costuma apontar referencia incorreta ou conversao invertida.",
  },
  UNIDADE_ORIGEM_VAZIA: {
    label: "Unidade ausente",
    helper: "Sem unidade de origem, o fator perde rastreabilidade operacional.",
  },
  MULTIPLAS_UNIDADES: {
    label: "Muitas unidades",
    helper: "Sinaliza produto/ano com excesso de unidades disputando a mesma chave.",
  },
  ALTA_VARIACAO_FATORES: {
    label: "Alta variacao",
    helper: "Pode indicar mistura de produtos diferentes sob a mesma chave.",
  },
};

function normalizeText(value: unknown): string {
  return String(value ?? "").trim();
}

function formatCount(value: number): string {
  return new Intl.NumberFormat("pt-BR").format(value);
}

function formatMetric(value: number | null | undefined): string {
  if (value == null || Number.isNaN(Number(value))) return "-";
  const num = Number(value);
  if (Math.abs(num) >= 1000 || (Math.abs(num) > 0 && Math.abs(num) < 0.01)) {
    return num.toExponential(2).replace(".", ",");
  }
  return num.toLocaleString("pt-BR", {
    minimumFractionDigits: Number.isInteger(num) ? 0 : 2,
    maximumFractionDigits: 4,
  });
}

function formatIssueType(tipo: string): string {
  return ISSUE_TYPE_META[tipo]?.label || normalizeText(tipo).replaceAll("_", " ");
}

function issueMatchesSearch(issue: FatorDiagnosticoItem, search: string): boolean {
  if (!search) return true;
  const haystack = [
    issue.tipo,
    issue.severidade,
    issue.chave_produto,
    issue.ano_referencia,
    issue.unidade_origem,
    issue.detalhes,
    issue.sugestao,
  ]
    .map((value) => normalizeText(value).toLowerCase())
    .join(" ");
  return haystack.includes(search.toLowerCase());
}

function getSeverityBadgeClass(severidade: string): string {
  return SEVERITY_META[severidade as Exclude<SeverityFilter, "todos">]?.badgeClass || "border-slate-200 bg-slate-100 text-slate-700";
}

function getSeverityRowClass(severidade: string): string {
  return SEVERITY_META[severidade as Exclude<SeverityFilter, "todos">]?.rowClass || "hover:bg-slate-50/80";
}

function SeverityShortcut({
  active,
  count,
  label,
  onClick,
  tone,
}: {
  active: boolean;
  count: number;
  label: string;
  onClick: () => void;
  tone: string;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={
        active
          ? `${tone} rounded-full border px-3 py-1.5 text-xs font-semibold shadow-sm`
          : "rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-600 hover:border-slate-300 hover:bg-slate-50"
      }
    >
      {label} ({formatCount(count)})
    </button>
  );
}

function StatCard({
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
    <div className={accent ? "rounded-xl border border-emerald-300 bg-emerald-50 px-4 py-3 shadow-sm" : "rounded-xl border border-slate-200 bg-white px-4 py-3"}>
      <div className={`text-[10px] font-black uppercase tracking-[0.18em] ${accent ? "text-emerald-700" : "text-slate-500"}`}>{label}</div>
      <div className={`mt-2 text-2xl font-black ${accent ? "text-emerald-900" : "text-slate-900"}`}>{formatCount(value)}</div>
      <div className={`mt-1 text-xs leading-5 ${accent ? "text-emerald-800" : "text-slate-500"}`}>{helper}</div>
    </div>
  );
}

export default function RevisaoFatores() {
  const [, navigate] = useLocation();
  const searchParams = new URLSearchParams(window.location.search);
  const cnpj = normalizeText(searchParams.get("cnpj") || "");
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const [activeTab, setActiveTab] = useState<ActiveTab>("ocorrencias");
  const [severityFilter, setSeverityFilter] = useState<SeverityFilter>("todos");
  const [typeFilter, setTypeFilter] = useState<string>("TODOS");
  const [search, setSearch] = useState("");
  const [calculating, setCalculating] = useState(false);
  const [importing, setImporting] = useState(false);

  const diagnosticoQuery = useQuery({
    queryKey: ["fatores-diagnostico", cnpj],
    queryFn: () => diagnosticarFatoresConversao(cnpj),
    enabled: Boolean(cnpj),
    staleTime: 30_000,
    retry: false,
  });

  const diagnostico = diagnosticoQuery.data || null;
  const diagnosticoError = diagnosticoQuery.error as (Error & { status?: number }) | null;
  const fatoresMissing = diagnostico ? !diagnostico.available : diagnosticoError?.status === 404;
  const issues = diagnostico?.issues || [];

  const severityCounts = useMemo(
    () =>
      issues.reduce(
        (acc, issue) => {
          const key = normalizeText(issue.severidade).toLowerCase() as Exclude<SeverityFilter, "todos">;
          if (key in acc) acc[key] += 1;
          return acc;
        },
        { critico: 0, alto: 0, medio: 0, baixo: 0 }
      ),
    [issues]
  );

  const issueTypeCounts = useMemo(() => {
    const counts = new Map<string, number>();
    issues.forEach((issue) => {
      const key = normalizeText(issue.tipo);
      counts.set(key, (counts.get(key) || 0) + 1);
    });
    return Array.from(counts.entries()).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
  }, [issues]);

  const filteredIssues = useMemo(
    () =>
      issues.filter((issue) => {
        const severityMatches = severityFilter === "todos" || normalizeText(issue.severidade).toLowerCase() === severityFilter;
        const typeMatches = typeFilter === "TODOS" || normalizeText(issue.tipo) === typeFilter;
        return severityMatches && typeMatches && issueMatchesSearch(issue, search);
      }),
    [issues, severityFilter, typeFilter, search]
  );

  const openFactorsFile = (filePath?: string) => {
    const normalized = normalizeText(filePath).replace(/\\/g, "/");
    if (!normalized) return;
    window.open(`/tabelas/view?file_path=${encodeURIComponent(normalized)}`, "_blank");
  };

  const handleRefresh = async () => {
    try {
      const result = await diagnosticoQuery.refetch();
      if (result.error) throw result.error;
      toast.success("Diagnostico atualizado.");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Falha ao atualizar o diagnostico.");
    }
  };

  const handleRecalculate = async () => {
    if (!cnpj) return;
    setCalculating(true);
    try {
      const result = await calcularFatoresConversao(cnpj);
      toast.success("Fatores recalculados.", {
        description: `${formatCount(result.qtd_registros || 0)} registro(s) processado(s).`,
      });
      await diagnosticoQuery.refetch();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Falha ao recalcular fatores.");
    } finally {
      setCalculating(false);
    }
  };

  const handleImportExcel = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    event.target.value = "";
    if (!file || !cnpj) return;

    setImporting(true);
    try {
      const result = await importFatoresExcel(cnpj, file);
      toast.success("Fatores importados.", {
        description: `${formatCount(result.registros || 0)} registro(s) atualizados a partir do Excel.`,
      });
      await diagnosticoQuery.refetch();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Falha ao importar fatores.");
    } finally {
      setImporting(false);
    }
  };

  if (!cnpj) {
    return (
      <div className="container mx-auto py-6">
        <Empty className="min-h-[60vh] border border-dashed border-slate-200 bg-white">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <FileSpreadsheet />
            </EmptyMedia>
            <EmptyTitle>Revisao de fatores sem CNPJ</EmptyTitle>
            <EmptyDescription>
              Abra esta tela a partir do resultado da auditoria ou informe o parametro <code>cnpj</code> na URL.
            </EmptyDescription>
          </EmptyHeader>
          <Button onClick={() => navigate("/auditar")}>Ir para auditoria</Button>
        </Empty>
      </div>
    );
  }

  return (
    <div className="container mx-auto space-y-6 py-6">
      <input
        ref={fileInputRef}
        type="file"
        accept=".xlsx,.xls"
        className="hidden"
        onChange={(event) => void handleImportExcel(event)}
      />

      <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <Button variant="ghost" size="icon" className="h-8 w-8" onClick={() => window.history.back()} aria-label="Voltar" title="Voltar">
              <ChevronLeft className="h-4 w-4" />
            </Button>
            <h1 className="text-2xl font-bold tracking-tight text-slate-900">Revisao operacional de fatores</h1>
          </div>
          <div className="flex flex-wrap items-center gap-2 text-sm text-slate-600">
            <Badge variant="outline" className="font-mono bg-slate-50">
              {cnpj}
            </Badge>
            <span>Diagnostico de fragilidades na conversao de unidades para triagem operacional.</span>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Button variant="outline" className="gap-2" onClick={() => void handleRecalculate()} disabled={calculating}>
            {calculating ? <Loader2 className="h-4 w-4 animate-spin" /> : <Wrench className="h-4 w-4" />}
            {fatoresMissing ? "Calcular fatores" : "Recalcular fatores"}
          </Button>
          <Button
            variant="outline"
            className="gap-2"
            onClick={() => fileInputRef.current?.click()}
            disabled={importing || fatoresMissing || !diagnostico?.file}
          >
            {importing ? <Loader2 className="h-4 w-4 animate-spin" /> : <Upload className="h-4 w-4" />}
            Importar Excel
          </Button>
          <Button
            variant="outline"
            className="gap-2"
            onClick={() => openFactorsFile(diagnostico?.file)}
            disabled={!diagnostico?.file}
          >
            <FolderOpen className="h-4 w-4" />
            Abrir parquet
          </Button>
          <Button
            variant="outline"
            className="gap-2"
            onClick={() => void handleRefresh()}
            disabled={diagnosticoQuery.isFetching}
          >
            <RefreshCw className={`h-4 w-4 ${diagnosticoQuery.isFetching ? "animate-spin" : ""}`} />
            Atualizar diagnostico
          </Button>
        </div>
      </div>

      <Separator />

      {diagnosticoQuery.isLoading ? (
        <div className="flex min-h-[45vh] flex-col items-center justify-center gap-3 rounded-2xl border border-slate-200 bg-white">
          <Loader2 className="h-10 w-10 animate-spin text-slate-500" />
          <p className="text-sm text-slate-500">Carregando diagnostico de fatores...</p>
        </div>
      ) : null}

      {!diagnosticoQuery.isLoading && fatoresMissing ? (
        <Card className="border-amber-300 bg-amber-50/70">
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-amber-900">
              <AlertTriangle className="h-5 w-5" />
              Arquivo de fatores ainda nao foi gerado
            </CardTitle>
            <CardDescription className="text-amber-900/80">
              Gere os fatores de conversao para liberar a triagem operacional e, se necessario, depois importe ajustes via Excel.
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-wrap items-center gap-3">
            <Button className="gap-2 bg-amber-600 text-white hover:bg-amber-700" onClick={() => void handleRecalculate()} disabled={calculating}>
              {calculating ? <Loader2 className="h-4 w-4 animate-spin" /> : <Wrench className="h-4 w-4" />}
              Calcular fatores agora
            </Button>
            <span className="text-sm text-amber-900/80">Depois do calculo, esta tela passa a mostrar risco por severidade, tipo e produto/ano.</span>
          </CardContent>
        </Card>
      ) : null}

      {!diagnosticoQuery.isLoading && !fatoresMissing && diagnosticoError ? (
        <Alert variant="destructive">
          <ShieldAlert className="h-4 w-4" />
          <AlertTitle>Falha ao carregar o diagnostico de fatores</AlertTitle>
          <AlertDescription>
            <p>{diagnosticoError.message}</p>
          </AlertDescription>
        </Alert>
      ) : null}

      {!diagnosticoQuery.isLoading && diagnostico ? (
        <>
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
            <StatCard
              label="Registros"
              value={diagnostico.stats.total_registros}
              helper="Total de linhas avaliadas no parquet de fatores."
            />
            <StatCard
              label="Produtos"
              value={diagnostico.stats.produtos_unicos}
              helper="Produtos distintos com fatores cadastrados."
            />
            <StatCard
              label="Editados manual"
              value={diagnostico.stats.editados_manual}
              helper="Linhas ja alteradas fora do calculo automatico."
              accent
            />
            <StatCard
              label="Criticos"
              value={severityCounts.critico}
              helper="Fatores invalidos ou extremos altos que pedem acao imediata."
            />
            <StatCard
              label="Alta variacao"
              value={diagnostico.stats.grupos_alta_variacao}
              helper="Produto/ano com divergencia relevante entre fatores."
            />
            <StatCard
              label="Muitas unidades"
              value={diagnostico.stats.grupos_muitas_unidades}
              helper="Produto/ano com excesso de unidades concorrentes."
            />
          </div>

          <Tabs value={activeTab} onValueChange={(value) => setActiveTab(value as ActiveTab)} className="gap-4">
            <TabsList className="w-full justify-start sm:w-auto">
              <TabsTrigger value="ocorrencias">Ocorrencias</TabsTrigger>
              <TabsTrigger value="resumo">Resumo operacional</TabsTrigger>
            </TabsList>

            <TabsContent value="ocorrencias" className="space-y-4">
              <Card className="border-slate-200 shadow-sm">
                <CardHeader className="pb-4">
                  <CardTitle className="text-lg text-slate-900">Fila de revisao</CardTitle>
                  <CardDescription>
                    Filtre por severidade, tipo ou texto livre. O backend ja entrega os casos ordenados por gravidade.
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="grid gap-3 xl:grid-cols-[1.25fr_1fr]">
                    <div className="space-y-2">
                      <div className="text-[11px] font-black uppercase tracking-[0.18em] text-slate-500">Busca rapida</div>
                      <div className="relative">
                        <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
                        <Input
                          value={search}
                          onChange={(event) => setSearch(event.target.value)}
                          placeholder="Produto, ano, unidade, detalhe ou sugestao"
                          className="pl-9"
                        />
                      </div>
                    </div>

                    <div className="space-y-2">
                      <div className="text-[11px] font-black uppercase tracking-[0.18em] text-slate-500">Severidade</div>
                      <div className="flex flex-wrap gap-2">
                        <SeverityShortcut
                          active={severityFilter === "todos"}
                          count={issues.length}
                          label="Todos"
                          onClick={() => setSeverityFilter("todos")}
                          tone="border-slate-300 bg-slate-700 text-white"
                        />
                        <SeverityShortcut
                          active={severityFilter === "critico"}
                          count={severityCounts.critico}
                          label="Critico"
                          onClick={() => setSeverityFilter("critico")}
                          tone={SEVERITY_META.critico.chipClass}
                        />
                        <SeverityShortcut
                          active={severityFilter === "alto"}
                          count={severityCounts.alto}
                          label="Alto"
                          onClick={() => setSeverityFilter("alto")}
                          tone={SEVERITY_META.alto.chipClass}
                        />
                        <SeverityShortcut
                          active={severityFilter === "medio"}
                          count={severityCounts.medio}
                          label="Medio"
                          onClick={() => setSeverityFilter("medio")}
                          tone={SEVERITY_META.medio.chipClass}
                        />
                      </div>
                    </div>
                  </div>

                  <div className="space-y-2">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <div className="text-[11px] font-black uppercase tracking-[0.18em] text-slate-500">Tipo de fragilidade</div>
                      {(typeFilter !== "TODOS" || severityFilter !== "todos" || search) ? (
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-8 px-2 text-xs text-slate-500"
                          onClick={() => {
                            setTypeFilter("TODOS");
                            setSeverityFilter("todos");
                            setSearch("");
                          }}
                        >
                          Limpar filtros
                        </Button>
                      ) : null}
                    </div>
                    <div className="flex flex-wrap gap-2">
                      <button
                        type="button"
                        onClick={() => setTypeFilter("TODOS")}
                        className={
                          typeFilter === "TODOS"
                            ? "rounded-full border border-slate-300 bg-slate-700 px-3 py-1.5 text-xs font-semibold text-white shadow-sm"
                            : "rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-600 hover:border-slate-300 hover:bg-slate-50"
                        }
                      >
                        Todos ({formatCount(issues.length)})
                      </button>
                      {issueTypeCounts.map(([tipo, count]) => (
                        <button
                          key={tipo}
                          type="button"
                          onClick={() => setTypeFilter(tipo)}
                          className={
                            typeFilter === tipo
                              ? "rounded-full border border-blue-300 bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white shadow-sm"
                              : "rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-600 hover:border-slate-300 hover:bg-slate-50"
                          }
                        >
                          {formatIssueType(tipo)} ({formatCount(count)})
                        </button>
                      ))}
                    </div>
                  </div>
                </CardContent>
              </Card>
              <Card className="overflow-hidden border-slate-200 shadow-sm">
                <CardHeader className="border-b bg-slate-50/80">
                  <CardTitle className="text-lg text-slate-900">Ocorrencias detalhadas</CardTitle>
                  <CardAction>
                    <Badge variant="outline" className="bg-white">
                      {formatCount(filteredIssues.length)} visiveis / {formatCount(issues.length)} total
                    </Badge>
                  </CardAction>
                </CardHeader>
                <CardContent className="p-0">
                  {filteredIssues.length === 0 ? (
                    <Empty className="min-h-[320px] rounded-none border-0">
                      <EmptyHeader>
                        <EmptyMedia variant="icon">
                          <FileSpreadsheet />
                        </EmptyMedia>
                        <EmptyTitle>Nenhuma ocorrencia para os filtros atuais</EmptyTitle>
                        <EmptyDescription>
                          Ajuste a severidade, o tipo ou a busca para voltar a listar as fragilidades do arquivo.
                        </EmptyDescription>
                      </EmptyHeader>
                    </Empty>
                  ) : (
                    <div className="max-h-[68vh] overflow-auto">
                      <Table className="min-w-[1024px]">
                        <TableHeader className="sticky top-0 z-10 bg-slate-100/95 backdrop-blur">
                          <TableRow>
                            <TableHead className="px-4 py-3 text-[11px] font-black uppercase tracking-[0.14em] text-slate-500">Severidade</TableHead>
                            <TableHead className="px-4 py-3 text-[11px] font-black uppercase tracking-[0.14em] text-slate-500">Tipo</TableHead>
                            <TableHead className="px-4 py-3 text-[11px] font-black uppercase tracking-[0.14em] text-slate-500">Produto / Ano</TableHead>
                            <TableHead className="px-4 py-3 text-[11px] font-black uppercase tracking-[0.14em] text-slate-500">Unidade</TableHead>
                            <TableHead className="px-4 py-3 text-[11px] font-black uppercase tracking-[0.14em] text-slate-500">Fator / Variacao</TableHead>
                            <TableHead className="px-4 py-3 text-[11px] font-black uppercase tracking-[0.14em] text-slate-500">Detalhes e acao sugerida</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {filteredIssues.map((issue, index) => (
                            <TableRow key={`${issue.tipo}:${issue.chave_produto}:${issue.ano_referencia ?? "na"}:${issue.unidade_origem}:${index}`} className={getSeverityRowClass(issue.severidade)}>
                              <TableCell className="px-4 py-4 align-top">
                                <Badge variant="outline" className={getSeverityBadgeClass(issue.severidade)}>
                                  {SEVERITY_META[issue.severidade as Exclude<SeverityFilter, "todos">]?.label || normalizeText(issue.severidade)}
                                </Badge>
                              </TableCell>
                              <TableCell className="px-4 py-4 align-top">
                                <div className="font-semibold text-slate-900">{formatIssueType(issue.tipo)}</div>
                                <div className="mt-1 whitespace-normal text-xs leading-5 text-slate-500">
                                  {ISSUE_TYPE_META[issue.tipo]?.helper || "Fragilidade operacional detectada."}
                                </div>
                              </TableCell>
                              <TableCell className="px-4 py-4 align-top">
                                <div className="font-mono text-xs font-semibold text-slate-700">{normalizeText(issue.chave_produto) || "-"}</div>
                                <div className="mt-1 text-xs text-slate-500">Ano {issue.ano_referencia ?? "-"}</div>
                              </TableCell>
                              <TableCell className="px-4 py-4 align-top whitespace-normal text-sm text-slate-700">
                                {normalizeText(issue.unidade_origem) || "-"}
                              </TableCell>
                              <TableCell className="px-4 py-4 align-top">
                                <div className="font-mono text-sm font-semibold text-slate-900">{formatMetric(issue.fator ?? null)}</div>
                                <div className="mt-1 text-xs text-slate-500">
                                  {issue.tipo === "ALTA_VARIACAO_FATORES" ? "Variacao detectada no grupo" : "Valor observado no diagnostico"}
                                </div>
                              </TableCell>
                              <TableCell className="px-4 py-4 align-top whitespace-normal">
                                <div className="text-sm leading-6 text-slate-800">{normalizeText(issue.detalhes) || "-"}</div>
                                <div className="mt-2 rounded-lg border border-slate-200 bg-white/80 px-3 py-2 text-xs leading-5 text-slate-600">
                                  <strong className="text-slate-700">Sugerido:</strong> {normalizeText(issue.sugestao) || "Revisar este fator manualmente."}
                                </div>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  )}
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="resumo" className="space-y-4">
              <Alert className="border-sky-200 bg-sky-50 text-sky-950">
                <ShieldAlert className="h-4 w-4" />
                <AlertTitle>Leitura operacional recomendada</AlertTitle>
                <AlertDescription>
                  <p>Trate fatores como qualidade de dados, nao apenas como calculo. Comece por casos criticos, recalcule quando houver base nova e importe Excel so depois da curadoria.</p>
                </AlertDescription>
              </Alert>

              <div className="grid gap-4 xl:grid-cols-[1.3fr_0.9fr]">
                <Card className="border-slate-200 shadow-sm">
                  <CardHeader>
                    <CardTitle className="text-lg text-slate-900">Distribuicao por tipo</CardTitle>
                    <CardDescription>
                      Cada bloco funciona como atalho para filtrar a aba de ocorrencias.
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="grid gap-3 md:grid-cols-2">
                    {issueTypeCounts.length === 0 ? (
                      <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-500">
                        Nenhuma fragilidade mapeada no arquivo atual.
                      </div>
                    ) : (
                      issueTypeCounts.map(([tipo, count]) => (
                        <button
                          key={`summary:${tipo}`}
                          type="button"
                          onClick={() => {
                            setTypeFilter(tipo);
                            setActiveTab("ocorrencias");
                          }}
                          className={
                            typeFilter === tipo
                              ? "rounded-2xl border border-blue-300 bg-blue-50 px-4 py-4 text-left shadow-sm"
                              : "rounded-2xl border border-slate-200 bg-white px-4 py-4 text-left shadow-sm transition-colors hover:border-slate-300 hover:bg-slate-50"
                          }
                        >
                          <div className="flex items-center justify-between gap-3">
                            <div className="text-sm font-semibold text-slate-900">{formatIssueType(tipo)}</div>
                            <Badge variant="outline" className="bg-white">
                              {formatCount(count)}
                            </Badge>
                          </div>
                          <div className="mt-2 text-xs leading-5 text-slate-500">
                            {ISSUE_TYPE_META[tipo]?.helper || "Fragilidade operacional detectada."}
                          </div>
                        </button>
                      ))
                    )}
                  </CardContent>
                </Card>

                <Card className="border-slate-200 shadow-sm">
                  <CardHeader>
                    <CardTitle className="text-lg text-slate-900">Playbook rapido</CardTitle>
                    <CardDescription>
                      Sequencia sugerida para saneamento sem perder rastreabilidade.
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-3 text-sm text-slate-600">
                    <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3">
                      <div className="font-semibold text-red-800">1. Priorize criticos ({formatCount(severityCounts.critico)})</div>
                      <div className="mt-1 text-xs leading-5 text-red-700">Fator invalido e fator extremo alto devem ser revisados antes de confiar no custo medio.</div>
                    </div>
                    <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3">
                      <div className="font-semibold text-amber-900">2. Revise grupos com variacao alta</div>
                      <div className="mt-1 text-xs leading-5 text-amber-800">Use a chave do produto/ano para verificar se ha mistura de unidades ou agregacao indevida.</div>
                    </div>
                    <div className="rounded-xl border border-sky-200 bg-sky-50 px-4 py-3">
                      <div className="font-semibold text-sky-900">3. Corrija e reimporte quando necessario</div>
                      <div className="mt-1 text-xs leading-5 text-sky-800">Depois da curadoria, importe o Excel para reaplicar ajustes sem perder o arquivo base.</div>
                    </div>
                    <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
                      <div className="font-semibold text-slate-800">Arquivo atual</div>
                      <div className="mt-1 break-all font-mono text-[11px] leading-5 text-slate-500">{diagnostico.file}</div>
                    </div>
                  </CardContent>
                </Card>
              </div>
            </TabsContent>
          </Tabs>
        </>
      ) : null}
    </div>
  );
}
