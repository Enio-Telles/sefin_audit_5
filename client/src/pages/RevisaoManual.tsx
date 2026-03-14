import { useEffect, useMemo, useState } from "react";
import { keepPreviousData, useQuery, useQueryClient } from "@tanstack/react-query";
import { useLocation } from "wouter";
import {
  AlertCircle,
  ArrowUpDown,
  Boxes,
  CheckCircle2,
  ChevronLeft,
  GitMerge,
  Loader2,
  RefreshCw,
  SplitSquareHorizontal,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { Separator } from "@/components/ui/separator";
import { toast } from "sonner";
import {
  getCodigosMultiDescricao,
  getStatusAnaliseProdutos,
  marcarProdutoVerificado,
  autoSepararResidual,
  desfazerProdutoVerificado,
  type AutoSepararResidualMode,
  type AutoSepararResidualResponse,
  type ProdutoAnaliseStatusResumo,
} from "@/lib/pythonApi";
import { analyzeDescriptions } from "@/lib/productSimilarity";

type ReviewRow = Record<string, unknown>;
type BulkSeparateMode = AutoSepararResidualMode;
type ReasonFilterState = { mode: BulkSeparateMode; motivo: string };
type ReasonGroupFilter = "SIMILARIDADE" | "FISCAL" | null;

function normalizeValue(value: unknown): string {
  return String(value ?? "").trim();
}

function splitDescriptions(value: unknown): string[] {
  return normalizeValue(value)
    .split("<<#>>")
    .map((item) => item.trim())
    .filter(Boolean);
}

function buildDescrComplMap(value: unknown): Map<string, string> {
  return new Map(
    normalizeValue(value)
      .split("<<#>>")
      .map((item) => item.trim())
      .filter(Boolean)
      .map((item) => {
        const [descricao, descrCompl] = item.split(" ::: ");
        return [descricao?.trim() || "", descrCompl?.trim() || ""] as [string, string];
      })
      .filter(([descricao]) => Boolean(descricao))
  );
}

function sumMetric(rows: ReviewRow[], field: string): number {
  return rows.reduce((acc, row) => acc + Number(normalizeValue(row[field]) || 0), 0);
}

function summarizeIgnoredReasons(preview?: AutoSepararResidualResponse): string {
  if (!preview?.resumo_motivos_ignorados?.length) return "";
  return preview.resumo_motivos_ignorados
    .slice(0, 2)
    .map((item) => `${item.qtd_codigos}x ${item.motivo}`)
    .join(" | ");
}

function isSimilarityReason(reason: string): boolean {
  const text = normalizeValue(reason).toLowerCase();
  return text.includes("similaridade") || text.includes("textual");
}

function isFiscalReason(reason: string): boolean {
  const text = normalizeValue(reason).toUpperCase();
  return text.includes("NCM") || text.includes("CEST") || text.includes("GTIN");
}

function modeLabel(mode: BulkSeparateMode): string {
  return mode === "NCM_CEST_GTIN"
    ? "Texto + NCM + CEST + GTIN"
    : mode === "NCM_GTIN"
      ? "Texto + NCM + GTIN"
      : mode === "NCM_ONLY"
        ? "Texto + NCM"
        : "Somente texto";
}

function downloadCsv(filename: string, rows: Array<Record<string, unknown>>) {
  const headers = Array.from(
    rows.reduce((set, row) => {
      Object.keys(row).forEach((key) => set.add(key));
      return set;
    }, new Set<string>())
  );
  const escape = (value: unknown) => `"${String(value ?? "").replace(/"/g, '""')}"`;
  const csv = [headers.join(","), ...rows.map((row) => headers.map((header) => escape(row[header])).join(","))].join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}

function StatBox({ label, value, highlight = false }: { label: string; value: number; highlight?: boolean }) {
  return (
    <div
      className={
        highlight
          ? "rounded-lg border border-blue-300 bg-blue-50 px-4 py-3 shadow-sm"
          : "rounded-lg border border-slate-200 bg-white px-4 py-3"
      }
    >
      <div className={`text-[10px] font-black uppercase tracking-widest ${highlight ? "text-blue-700" : "text-slate-500"}`}>{label}</div>
      <div className={`mt-1 text-2xl font-black ${highlight ? "text-blue-900" : "text-slate-900"}`}>{value}</div>
    </div>
  );
}

export default function RevisaoManual() {
  const [, navigate] = useLocation();
  const searchParams = new URLSearchParams(window.location.search);
  const cnpj = searchParams.get("cnpj") || "";
  const storageKey = `produto-revisao-residual-ui:${cnpj}`;

  const [rows, setRows] = useState<ReviewRow[]>([]);
  const [statusResumo, setStatusResumo] = useState<ProdutoAnaliseStatusResumo | null>(null);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [totalRows, setTotalRows] = useState(0);
  const [totalPages, setTotalPages] = useState(1);
  const [tableSummary, setTableSummary] = useState<{ total_codigos: number; total_descricoes: number; total_grupos: number } | null>(null);
  const [sortColumn, setSortColumn] = useState<string | undefined>("qtd_descricoes");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc" | undefined>("desc");
  const [autoSeparatingCode, setAutoSeparatingCode] = useState<string | null>(null);
  const [bulkSeparatingMode, setBulkSeparatingMode] = useState<BulkSeparateMode | null>(null);
  const [bulkPreview, setBulkPreview] = useState<Partial<Record<BulkSeparateMode, AutoSepararResidualResponse>>>({});
  const [activeReasonFilter, setActiveReasonFilter] = useState<ReasonFilterState | null>(null);
  const [reasonGroupFilter, setReasonGroupFilter] = useState<ReasonGroupFilter>(null);
  const [statusUpdatingCode, setStatusUpdatingCode] = useState<string | null>(null);
  const [showVerified, setShowVerified] = useState(false);
  const queryClient = useQueryClient();

  const resetUiState = () => {
      setSortColumn("qtd_descricoes");
      setSortDirection("desc");
      setShowVerified(false);
      setActiveReasonFilter(null);
      setReasonGroupFilter(null);
      setPage(1);
      setPageSize(50);
      window.sessionStorage.removeItem(storageKey);
    };

  useEffect(() => {
    if (!cnpj) return;
    try {
      const raw = window.sessionStorage.getItem(storageKey);
      if (!raw) return;
        const state = JSON.parse(raw) as {
          page?: number;
          pageSize?: number;
          sortColumn?: string;
          sortDirection?: "asc" | "desc";
          showVerified?: boolean;
        activeReasonFilter?: ReasonFilterState | null;
        reasonGroupFilter?: ReasonGroupFilter;
      };
        if (typeof state.page === "number" && state.page > 0) setPage(state.page);
        if (typeof state.pageSize === "number" && state.pageSize > 0) setPageSize(state.pageSize);
        if (typeof state.sortColumn === "string") setSortColumn(state.sortColumn);
      if (state.sortDirection === "asc" || state.sortDirection === "desc") setSortDirection(state.sortDirection);
      if (typeof state.showVerified === "boolean") setShowVerified(state.showVerified);
      if (state.activeReasonFilter && typeof state.activeReasonFilter.mode === "string" && typeof state.activeReasonFilter.motivo === "string") {
        setActiveReasonFilter(state.activeReasonFilter);
      }
      if (state.reasonGroupFilter === "SIMILARIDADE" || state.reasonGroupFilter === "FISCAL") {
        setReasonGroupFilter(state.reasonGroupFilter);
      }
    } catch {
      // ignore invalid session state
    }
  }, [cnpj, storageKey]);

  useEffect(() => {
    if (!cnpj) return;
    window.sessionStorage.setItem(
      storageKey,
        JSON.stringify({
          page,
          pageSize,
          sortColumn,
          sortDirection,
          showVerified,
        activeReasonFilter,
        reasonGroupFilter,
      })
    );
  }, [cnpj, storageKey, page, pageSize, sortColumn, sortDirection, showVerified, activeReasonFilter, reasonGroupFilter]);

  const reviewQuery = useQuery({
    queryKey: ["produto-revisao-residual", cnpj, page, pageSize, sortColumn, sortDirection, showVerified],
    queryFn: async () => {
      const [res, statusRes] = await Promise.all([
        getCodigosMultiDescricao(cnpj, {
          page,
          pageSize,
          sortColumn,
          sortDirection: sortDirection ?? "desc",
          showVerified,
        }),
        getStatusAnaliseProdutos(cnpj, { includeData: false }),
      ]);
      return { res, statusRes };
    },
    enabled: Boolean(cnpj),
    placeholderData: keepPreviousData,
    staleTime: 30_000,
    retry: 1,
  });

  const loading = reviewQuery.isLoading || reviewQuery.isFetching;

  const loadData = async () => {
    if (!cnpj) {
      setRows([]);
      setStatusResumo(null);
      return;
    }
    const payload = await reviewQuery.refetch();
    if (!payload.data) return;
    const { res, statusRes } = payload.data;
    setRows(res.success ? res.data : []);
    setPage(res.page || page);
    setPageSize(res.page_size || pageSize);
    setTotalRows(res.total || 0);
    setTotalPages(res.total_pages || 1);
    setTableSummary(res.summary || null);
    setStatusResumo(statusRes.success ? statusRes.resumo : null);
  };

  useEffect(() => {
    if (!reviewQuery.data) return;
    const { res, statusRes } = reviewQuery.data;
    setRows(res.success ? res.data : []);
    setPage(res.page || page);
    setPageSize(res.page_size || pageSize);
    setTotalRows(res.total || 0);
    setTotalPages(res.total_pages || 1);
    setTableSummary(res.summary || null);
    setStatusResumo(statusRes.success ? statusRes.resumo : null);
  }, [reviewQuery.data]);

  useEffect(() => {
    const handleAtualizacao = (event: MessageEvent) => {
      if (event.origin !== window.location.origin) return;
      if (event.data?.type !== "produto-revisao-atualizada") return;
      if (event.data?.cnpj !== cnpj) return;
      queryClient.invalidateQueries({ queryKey: ["produto-revisao-residual", cnpj] });
      void reviewQuery.refetch();
    };

    window.addEventListener("message", handleAtualizacao);
    return () => window.removeEventListener("message", handleAtualizacao);
  }, [cnpj, queryClient, reviewQuery]);

  const handleSort = (column: string) => {
    if (sortColumn === column) {
      setSortDirection((prev) => (prev === "asc" ? "desc" : prev === "desc" ? undefined : "asc"));
      if (sortDirection === "desc") {
        setSortColumn(undefined);
      }
      setPage(1);
      return;
    }
    setSortColumn(column);
    setSortDirection("asc");
    setPage(1);
  };

  const sortedRows = useMemo(() => {
    let data = [...rows];
    if (activeReasonFilter) {
      const ignored = bulkPreview[activeReasonFilter.mode]?.motivos_ignorados ?? [];
      const codigosFiltrados = new Set(
        ignored.filter((item) => item.motivo === activeReasonFilter.motivo).map((item) => normalizeValue(item.codigo))
      );
      data = data.filter((row) => codigosFiltrados.has(normalizeValue(row.codigo)));
    } else if (reasonGroupFilter) {
      const matchingCodes = new Set<string>();
      (Object.values(bulkPreview) as Array<AutoSepararResidualResponse | undefined>).forEach((preview) => {
        (preview?.motivos_ignorados ?? []).forEach((item) => {
          const matches =
            reasonGroupFilter === "SIMILARIDADE"
              ? isSimilarityReason(item.motivo)
              : isFiscalReason(item.motivo);
          if (matches) matchingCodes.add(normalizeValue(item.codigo));
        });
      });
      data = data.filter((row) => matchingCodes.has(normalizeValue(row.codigo)));
    }
    if (sortColumn === "__similaridade__" && sortDirection) {
      data = [...data].sort((a, b) => {
        const aScore = analyzeDescriptions(splitDescriptions(a.lista_descricoes)).maxSimilarity;
        const bScore = analyzeDescriptions(splitDescriptions(b.lista_descricoes)).maxSimilarity;
        const cmp = aScore - bScore;
        return sortDirection === "asc" ? cmp : -cmp;
      });
    }
    return data;
  }, [rows, activeReasonFilter, reasonGroupFilter, bulkPreview, sortColumn, sortDirection]);

  const visibleCodigos = useMemo(
    () => sortedRows.map((row) => normalizeValue(row.codigo)).filter(Boolean),
    [sortedRows]
  );

  useEffect(() => {
    if (!cnpj) return;
    let cancelled = false;
    const modes: BulkSeparateMode[] = ["NCM_CEST_GTIN", "NCM_GTIN", "NCM_ONLY", "TEXT_ONLY"];
    const loadPreviews = async () => {
      const entries = await Promise.all(
        modes.map(async (mode) => {
          try {
            const res = await autoSepararResidual(cnpj, mode, true, visibleCodigos);
            return [mode, res] as const;
          } catch {
            return [mode, undefined] as const;
          }
        })
      );
      if (!cancelled) {
        setBulkPreview(Object.fromEntries(entries));
      }
    };
    void loadPreviews();
    return () => {
      cancelled = true;
    };
  }, [cnpj, visibleCodigos]);

  const totalDescricoes = tableSummary?.total_descricoes ?? sumMetric(rows, "qtd_descricoes");
  const totalGrupos = tableSummary?.total_grupos ?? sumMetric(rows, "qtd_grupos_descricao_afetados");

  const verifiedByCodigo = useMemo(
    () => new Set(rows.filter((item) => normalizeValue(item.status_analise) === "VERIFICADO_SEM_ACAO").map((item) => normalizeValue(item.codigo))),
    [rows]
  );

  const similarityBlockedCount = useMemo(() => {
    const matchingCodes = new Set<string>();
    (Object.values(bulkPreview) as Array<AutoSepararResidualResponse | undefined>).forEach((preview) => {
      (preview?.motivos_ignorados ?? []).forEach((item) => {
        if (isSimilarityReason(item.motivo)) matchingCodes.add(normalizeValue(item.codigo));
      });
    });
    return rows.filter((row) => matchingCodes.has(normalizeValue(row.codigo))).length;
  }, [bulkPreview, rows]);

  const fiscalBlockedCount = useMemo(() => {
    const matchingCodes = new Set<string>();
    (Object.values(bulkPreview) as Array<AutoSepararResidualResponse | undefined>).forEach((preview) => {
      (preview?.motivos_ignorados ?? []).forEach((item) => {
        if (isFiscalReason(item.motivo)) matchingCodes.add(normalizeValue(item.codigo));
      });
    });
    return rows.filter((row) => matchingCodes.has(normalizeValue(row.codigo))).length;
  }, [bulkPreview, rows]);

  const toggleVerified = async (codigo: string, descricoes: string) => {
    setStatusUpdatingCode(codigo);
    try {
      if (verifiedByCodigo.has(codigo)) {
        const res = await desfazerProdutoVerificado({
          cnpj,
          tipo_ref: "POR_CODIGO",
          ref_id: codigo,
          descricao_ref: descricoes,
          contexto_tela: "REVISAO_RESIDUAL",
        });
        toast.success(res.mensagem);
      } else {
        const res = await marcarProdutoVerificado({
          cnpj,
          tipo_ref: "POR_CODIGO",
          ref_id: codigo,
          descricao_ref: descricoes,
          contexto_tela: "REVISAO_RESIDUAL",
        });
        toast.success(res.mensagem);
      }
      await loadData();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Erro ao atualizar status do codigo.");
    } finally {
      setStatusUpdatingCode(null);
    }
  };

  const executeAutoSeparate = async (codigo: string, mode: BulkSeparateMode) => autoSepararResidual(cnpj, mode, false, [codigo]);

  const handleAutoSeparateDissimilares = async (codigo: string, mode: BulkSeparateMode = "NCM_CEST_GTIN") => {
    setAutoSeparatingCode(codigo);
    try {
      const result = await executeAutoSeparate(codigo, mode);
      if ((result.qtd_codigos_aplicados || 0) === 0) {
        toast.error("Auto-separacao indisponivel", {
          description: result.motivos_ignorados?.[0]?.motivo || "O codigo nao atendeu ao criterio selecionado.",
        });
        return;
      }
      toast.success("Separacao automatica aplicada.");
      await loadData();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Erro ao separar descricoes dissimilares.");
    } finally {
      setAutoSeparatingCode(null);
    }
  };

  const handleBulkAutoSeparate = async (mode: BulkSeparateMode) => {
    const preview = bulkPreview[mode];
    const modeLabel =
      mode === "NCM_CEST_GTIN"
        ? "descricoes muito dissimilares com NCM, CEST e GTIN diferentes"
        : mode === "NCM_GTIN"
          ? "descricoes muito dissimilares com NCM e GTIN diferentes"
          : mode === "NCM_ONLY"
            ? "descricoes muito dissimilares com NCM diferente"
            : "descricoes muito dissimilares";

    if (!window.confirm(`Aplicar separacao automatica em lote para todos os codigos visiveis com criterio: ${modeLabel}?${preview ? `\n\nElegiveis estimados: ${preview.qtd_codigos_elegiveis}` : ""}`)) {
      return;
    }

    setBulkSeparatingMode(mode);
    try {
      const result = await autoSepararResidual(cnpj, mode, false, visibleCodigos);
      if ((result.qtd_codigos_aplicados || 0) === 0) {
        toast.error("Nenhum codigo elegivel para a separacao automatica em lote.", {
          description: `${result.qtd_codigos_ignorados} codigo(s) foram ignorados pelo criterio selecionado.`,
        });
        return;
      }

      toast.success("Separacao automatica em lote concluida.", {
        description: `${result.qtd_codigos_aplicados} codigo(s) separados e ${result.qtd_codigos_ignorados} ignorados.`,
      });
      await loadData();
      const previewRefresh = await autoSepararResidual(cnpj, mode, true);
      setBulkPreview((current) => ({ ...current, [mode]: previewRefresh }));
    } finally {
      setBulkSeparatingMode(null);
    }
  };

  const handleExportFiltered = () => {
    const exportRows = sortedRows.map((row) => ({
      codigo: normalizeValue(row.codigo),
      descricoes: normalizeValue(row.lista_descricoes),
      qtd_descricoes: normalizeValue(row.qtd_descricoes),
      qtd_grupos: normalizeValue(row.qtd_grupos_descricao_afetados),
      similaridade: analyzeDescriptions(splitDescriptions(row.lista_descricoes)).bucket,
      descr_compl: normalizeValue(row.lista_descr_compl),
      ncm: normalizeValue(row.lista_ncm),
      cest: normalizeValue(row.lista_cest),
      gtin: normalizeValue(row.lista_gtin),
    }));
    downloadCsv(`revisao_residual_filtrada_${cnpj}.csv`, exportRows);
  };

  const handleExportReasonFilter = () => {
    if (!activeReasonFilter && !reasonGroupFilter) {
      handleExportFiltered();
      return;
    }
    const rowsToExport = sortedRows.map((row) => ({
      codigo: normalizeValue(row.codigo),
      descricoes: normalizeValue(row.lista_descricoes),
      filtro: activeReasonFilter
        ? `${modeLabel(activeReasonFilter.mode)} :: ${activeReasonFilter.motivo}`
        : reasonGroupFilter === "SIMILARIDADE"
          ? "BLOQUEIO_SIMILARIDADE"
          : "BLOQUEIO_FISCAL",
    }));
    downloadCsv(`revisao_residual_bloqueios_${cnpj}.csv`, rowsToExport);
  };

  const handleMarkFilteredVerified = async (markAsVerified: boolean) => {
    const targetRows = sortedRows;
    if (targetRows.length === 0) {
      toast.error("Nao ha codigos visiveis para atualizar.");
      return;
    }
    if (
      !window.confirm(
        `${markAsVerified ? "Marcar" : "Desfazer"} verificado para ${targetRows.length} codigo(s) visiveis?`
      )
    ) {
      return;
    }

    setStatusUpdatingCode("__BULK__");
    try {
      await Promise.all(
        targetRows.map((row) =>
          (markAsVerified ? marcarProdutoVerificado : desfazerProdutoVerificado)({
            cnpj,
            tipo_ref: "POR_CODIGO",
            ref_id: normalizeValue(row.codigo),
            descricao_ref: normalizeValue(row.lista_descricoes),
            contexto_tela: "REVISAO_RESIDUAL",
          })
        )
      );
      toast.success(markAsVerified ? "Codigos filtrados marcados como verificados." : "Marcacao de verificado removida dos codigos filtrados.");
      await loadData();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Erro ao atualizar verificados filtrados.");
    } finally {
      setStatusUpdatingCode(null);
    }
  };

  if (!cnpj) {
    return (
      <div className="flex min-h-[70vh] flex-col items-center justify-center gap-4 px-4 text-center">
        <AlertCircle className="h-12 w-12 text-muted-foreground" />
        <div className="space-y-2">
          <h2 className="text-xl font-semibold">Nenhum CNPJ informado</h2>
          <p className="text-muted-foreground">Abra esta tela a partir do resultado da auditoria.</p>
        </div>
        <Button onClick={() => navigate("/auditar")}>Ir para auditoria</Button>
      </div>
    );
  }

  return (
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-5 px-4 py-5">
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div className="space-y-2">
          <Button variant="ghost" size="sm" className="-ml-3 w-fit gap-2" onClick={() => window.history.back()}>
            <ChevronLeft className="h-4 w-4" />
            Voltar
          </Button>
          <div>
            <h1 className="text-xl font-black text-slate-900">Revisao residual</h1>
            <p className="mt-1 text-sm text-slate-500">CNPJ {cnpj} | Residual de codigos com descricoes diferentes.</p>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Button variant="outline" className="gap-2" onClick={resetUiState}>
            Restaurar padrao
          </Button>
          <Button
            variant={showVerified ? "default" : "outline"}
            className="gap-2"
            onClick={() => {
              setShowVerified((prev) => !prev);
              setPage(1);
            }}
          >
            <CheckCircle2 className="h-4 w-4" />
            {showVerified ? "Ocultar verificados" : "Mostrar verificados"}
          </Button>
          <Button variant="outline" className="gap-2" onClick={() => navigate(`/revisao-pares-grupos?cnpj=${encodeURIComponent(cnpj)}`)}>
            <GitMerge className="h-4 w-4" />
            Decisao entre grupos
          </Button>
          <Button variant="outline" className="gap-2" onClick={() => void loadData()}>
            {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
            Atualizar
          </Button>
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-3">
        <StatBox label="Codigos pendentes" value={tableSummary?.total_codigos ?? totalRows} />
        <StatBox label="Descricoes envolvidas" value={totalDescricoes} />
        <StatBox label="Grupos afetados" value={totalGrupos} />
      </div>

      <div className="grid gap-3 md:grid-cols-5">
        <StatBox label="Pendentes" value={statusResumo?.pendentes ?? 0} highlight />
        <StatBox label="Verificados" value={statusResumo?.verificados ?? 0} />
        <StatBox label="Consolidados" value={statusResumo?.consolidados ?? 0} />
        <StatBox label="Separados" value={statusResumo?.separados ?? 0} />
        <StatBox label="Decididos entre grupos" value={statusResumo?.decididos_entre_grupos ?? 0} />
      </div>

      <div className="overflow-hidden rounded-xl border border-slate-200 bg-white">
        <div className="border-b bg-slate-50 px-4 py-3">
          <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
            <div>
              <div className="text-sm font-black text-slate-900">Codigos com multiplas descricoes</div>
              <div className="mt-1 text-xs text-slate-500">
                Consolidar define uma descricao canonica para o codigo. Separar divide o codigo em novos produtos.
              </div>
            </div>
            <div className="text-xs font-semibold text-slate-500">
              Pagina {page}/{totalPages} · {sortedRows.length} itens visiveis · {totalRows} no total
            </div>
          </div>
          <div className="mt-3 flex flex-wrap gap-2">
            <Button
              size="sm"
              variant="outline"
              disabled={loading || bulkSeparatingMode !== null || (bulkPreview.NCM_CEST_GTIN?.qtd_codigos_elegiveis ?? 0) === 0}
              onClick={() => void handleBulkAutoSeparate("NCM_CEST_GTIN")}
            >
              {bulkSeparatingMode === "NCM_CEST_GTIN" ? <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" /> : null}
              Separar todos: texto + NCM + CEST + GTIN ({bulkPreview.NCM_CEST_GTIN?.qtd_codigos_elegiveis ?? 0})
            </Button>
            <Button
              size="sm"
              variant="outline"
              disabled={loading || bulkSeparatingMode !== null || (bulkPreview.NCM_GTIN?.qtd_codigos_elegiveis ?? 0) === 0}
              onClick={() => void handleBulkAutoSeparate("NCM_GTIN")}
            >
              {bulkSeparatingMode === "NCM_GTIN" ? <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" /> : null}
              Separar todos: texto + NCM + GTIN ({bulkPreview.NCM_GTIN?.qtd_codigos_elegiveis ?? 0})
            </Button>
            <Button
              size="sm"
              variant="outline"
              disabled={loading || bulkSeparatingMode !== null || (bulkPreview.NCM_ONLY?.qtd_codigos_elegiveis ?? 0) === 0}
              onClick={() => void handleBulkAutoSeparate("NCM_ONLY")}
            >
              {bulkSeparatingMode === "NCM_ONLY" ? <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" /> : null}
              Separar todos: texto + NCM ({bulkPreview.NCM_ONLY?.qtd_codigos_elegiveis ?? 0})
            </Button>
            <Button
              size="sm"
              variant="outline"
              disabled={loading || bulkSeparatingMode !== null || (bulkPreview.TEXT_ONLY?.qtd_codigos_elegiveis ?? 0) === 0}
              onClick={() => void handleBulkAutoSeparate("TEXT_ONLY")}
            >
              {bulkSeparatingMode === "TEXT_ONLY" ? <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" /> : null}
              Separar todos: somente texto muito diferente ({bulkPreview.TEXT_ONLY?.qtd_codigos_elegiveis ?? 0})
            </Button>
          </div>
          <div className="mt-2 grid gap-1 text-[11px] text-slate-500 md:grid-cols-2">
            <div>
              <strong className="text-slate-600">Texto + NCM + CEST + GTIN:</strong>{" "}
              {summarizeIgnoredReasons(bulkPreview.NCM_CEST_GTIN) || "Sem bloqueios resumidos."}
            </div>
            <div>
              <strong className="text-slate-600">Texto + NCM + GTIN:</strong>{" "}
              {summarizeIgnoredReasons(bulkPreview.NCM_GTIN) || "Sem bloqueios resumidos."}
            </div>
            <div>
              <strong className="text-slate-600">Texto + NCM:</strong>{" "}
              {summarizeIgnoredReasons(bulkPreview.NCM_ONLY) || "Sem bloqueios resumidos."}
            </div>
            <div>
              <strong className="text-slate-600">Somente texto:</strong>{" "}
              {summarizeIgnoredReasons(bulkPreview.TEXT_ONLY) || "Sem bloqueios resumidos."}
            </div>
          </div>
          <div className="mt-2 flex flex-wrap gap-2">
            <button
              type="button"
              onClick={() => {
                setActiveReasonFilter(null);
                setReasonGroupFilter((current) => (current === "SIMILARIDADE" ? null : "SIMILARIDADE"));
              }}
              className={
                reasonGroupFilter === "SIMILARIDADE"
                  ? "rounded-full border border-blue-400 bg-blue-50 px-3 py-1 text-[11px] font-semibold text-blue-800"
                  : "rounded-full border border-slate-200 bg-white px-3 py-1 text-[11px] text-slate-600 hover:border-slate-300"
              }
            >
              Ver so bloqueados por similaridade ({similarityBlockedCount})
            </button>
            <button
              type="button"
              onClick={() => {
                setActiveReasonFilter(null);
                setReasonGroupFilter((current) => (current === "FISCAL" ? null : "FISCAL"));
              }}
              className={
                reasonGroupFilter === "FISCAL"
                  ? "rounded-full border border-emerald-400 bg-emerald-50 px-3 py-1 text-[11px] font-semibold text-emerald-800"
                  : "rounded-full border border-slate-200 bg-white px-3 py-1 text-[11px] text-slate-600 hover:border-slate-300"
              }
            >
              Ver so bloqueios fiscais ({fiscalBlockedCount})
            </button>
            {(["NCM_CEST_GTIN", "NCM_GTIN", "NCM_ONLY", "TEXT_ONLY"] as BulkSeparateMode[]).flatMap((mode) =>
              (bulkPreview[mode]?.resumo_motivos_ignorados ?? []).slice(0, 3).map((item) => {
                const active = activeReasonFilter?.mode === mode && activeReasonFilter?.motivo === item.motivo;
                return (
                  <button
                    key={`${mode}:${item.motivo}`}
                    type="button"
                    onClick={() => {
                      setReasonGroupFilter(null);
                      setActiveReasonFilter(active ? null : { mode, motivo: item.motivo });
                    }}
                    className={
                      active
                        ? "rounded-full border border-amber-400 bg-amber-50 px-3 py-1 text-[11px] font-semibold text-amber-800"
                        : "rounded-full border border-slate-200 bg-white px-3 py-1 text-[11px] text-slate-600 hover:border-slate-300"
                    }
                    title={item.codigos_amostra?.length ? `Exemplos: ${item.codigos_amostra.join(", ")}` : modeLabel(mode)}
                  >
                    {modeLabel(mode)}: {item.motivo} ({item.qtd_codigos})
                  </button>
                );
              })
            )}
            {activeReasonFilter ? (
              <button
                type="button"
                onClick={() => {
                  setActiveReasonFilter(null);
                  setReasonGroupFilter(null);
                }}
                className="rounded-full border border-slate-300 bg-slate-50 px-3 py-1 text-[11px] text-slate-700"
              >
                Limpar filtro de bloqueio
              </button>
            ) : null}
            <button
              type="button"
              onClick={handleExportFiltered}
              className="rounded-full border border-slate-300 bg-slate-50 px-3 py-1 text-[11px] text-slate-700"
            >
              Exportar lista visivel
            </button>
            <button
              type="button"
              onClick={() => void handleMarkFilteredVerified(true)}
              className="rounded-full border border-slate-300 bg-slate-50 px-3 py-1 text-[11px] text-slate-700"
              disabled={statusUpdatingCode === "__BULK__" || sortedRows.length === 0}
            >
              {statusUpdatingCode === "__BULK__" ? "Atualizando..." : "Marcar visiveis como verificados"}
            </button>
            <button
              type="button"
              onClick={() => void handleMarkFilteredVerified(false)}
              className="rounded-full border border-slate-300 bg-slate-50 px-3 py-1 text-[11px] text-slate-700"
              disabled={statusUpdatingCode === "__BULK__" || sortedRows.length === 0}
            >
              {statusUpdatingCode === "__BULK__" ? "Atualizando..." : "Desfazer verificados visiveis"}
            </button>
            <button
              type="button"
              onClick={handleExportReasonFilter}
              className="rounded-full border border-slate-300 bg-slate-50 px-3 py-1 text-[11px] text-slate-700"
            >
              Exportar bloqueios filtrados
            </button>
          </div>
            <div className="mt-2 text-xs text-slate-500">
              Ordem de agressividade: <strong>texto + NCM + CEST + GTIN</strong> é a mais conservadora. <strong>somente texto muito diferente</strong> cobre mais casos, mas aumenta o risco.
            </div>
            <div className="mt-3 flex flex-wrap items-center justify-between gap-2 border-t pt-3">
              <div className="flex items-center gap-2 text-xs text-slate-500">
                <span>Linhas por pagina</span>
                <select
                  className="h-8 rounded-md border border-slate-300 bg-white px-2 text-xs"
                  value={pageSize}
                  onChange={(event) => {
                    setPageSize(Number(event.target.value));
                    setPage(1);
                  }}
                >
                  {[25, 50, 100].map((value) => (
                    <option key={value} value={value}>
                      {value}
                    </option>
                  ))}
                </select>
              </div>
              <div className="flex items-center gap-2">
                <Button size="sm" variant="outline" disabled={page <= 1 || loading} onClick={() => setPage((current) => Math.max(1, current - 1))}>
                  Anterior
                </Button>
                <span className="text-xs text-slate-500">
                  Pagina {page} de {totalPages}
                </span>
                <Button size="sm" variant="outline" disabled={page >= totalPages || loading} onClick={() => setPage((current) => Math.min(totalPages, current + 1))}>
                  Proxima
                </Button>
              </div>
            </div>
          </div>

        {loading ? (
          <div className="flex flex-col items-center justify-center gap-3 py-20">
            <Loader2 className="h-10 w-10 animate-spin text-slate-500" />
            <p className="text-sm text-muted-foreground">Carregando codigos multidescricao...</p>
          </div>
        ) : sortedRows.length === 0 ? (
          <div className="flex flex-col items-center justify-center gap-3 px-4 py-20 text-center">
            <Boxes className="h-10 w-10 text-emerald-600" />
            <div className="space-y-1">
              <h3 className="text-lg font-medium text-slate-900">Nenhum codigo ambiguo restante</h3>
              <p className="text-sm text-muted-foreground">Nao existem codigos associados a multiplas descricoes para este CNPJ.</p>
            </div>
          </div>
        ) : (
          <div className="overflow-auto">
            <table className="w-full min-w-[980px] border-collapse text-sm">
              <thead className="sticky top-0 z-10 bg-slate-50">
                <tr className="border-b">
                  {[
                    { key: "codigo", label: "Codigo" },
                    { key: "lista_descricoes", label: "Descricoes" },
                    { key: "qtd_descricoes", label: "Qtd. desc." },
                    { key: "qtd_grupos_descricao_afetados", label: "Qtd. grupos" },
                    { key: "__similaridade__", label: "Similaridade" },
                    { key: "lista_descr_compl", label: "Descr. compl." },
                    { key: "lista_ncm", label: "NCM" },
                    { key: "lista_cest", label: "CEST" },
                    { key: "lista_gtin", label: "GTIN" },
                  ].map((col) => (
                    <th key={col.key} className="px-4 py-3 text-left font-medium text-slate-700">
                      <button className="flex items-center gap-1 transition-colors hover:text-slate-950" onClick={() => handleSort(col.key)}>
                        {col.label}
                        <ArrowUpDown className={`h-3.5 w-3.5 ${sortColumn === col.key ? "text-slate-700" : "text-slate-300"}`} />
                      </button>
                    </th>
                  ))}
                  <th className="sticky right-0 bg-slate-50 px-4 py-3 text-center font-medium text-slate-700">Acoes</th>
                </tr>
              </thead>
              <tbody>
                {sortedRows.map((row, index) => (
                  <tr key={index} className="border-b align-top hover:bg-slate-50/70">
                    <td className="px-4 py-4 font-mono text-xs font-semibold text-slate-700">{normalizeValue(row.codigo)}</td>
                    <td className="max-w-xl px-4 py-4 text-slate-900">
                      <div className="space-y-2">
                        {(() => {
                          const descrComplMap = buildDescrComplMap(row.lista_descricoes_compl);
                          return splitDescriptions(row.lista_descricoes).map((desc, descIndex) => {
                            const descrCompl = descrComplMap.get(desc) || "";
                            return (
                              <div key={descIndex} className={descIndex > 0 ? "border-t pt-2 text-slate-500" : "font-medium"}>
                                <div>{desc}</div>
                                {descrCompl ? (
                                  <div className="mt-1 text-xs text-slate-500">
                                    Compl.: {descrCompl}
                                  </div>
                                ) : null}
                              </div>
                            );
                          });
                        })()}
                      </div>
                      <div className="mt-2 text-xs text-slate-500">Grupos: {normalizeValue(row.lista_chave_produto)}</div>
                    </td>
                    <td className="px-4 py-4 text-center font-mono text-xs text-slate-600">{normalizeValue(row.qtd_descricoes)}</td>
                    <td className="px-4 py-4 text-center font-mono text-xs text-slate-600">{normalizeValue(row.qtd_grupos_descricao_afetados)}</td>
                    <td className="px-4 py-4 text-xs text-slate-600">
                      {(() => {
                        const analysis = analyzeDescriptions(splitDescriptions(row.lista_descricoes));
                        return (
                          <div className="space-y-1">
                            <div className="font-medium text-slate-700">{analysis.bucket}</div>
                            <div>Max.: {(analysis.maxSimilarity * 100).toFixed(0)}%</div>
                            <div>Min.: {(analysis.minSimilarity * 100).toFixed(0)}%</div>
                          </div>
                        );
                      })()}
                    </td>
                    <td className="px-4 py-4 text-xs text-slate-600 whitespace-pre-wrap">{normalizeValue(row.lista_descr_compl)}</td>
                    <td className="px-4 py-4 text-xs text-slate-600">{normalizeValue(row.lista_ncm)}</td>
                    <td className="px-4 py-4 text-xs text-slate-600">{normalizeValue(row.lista_cest)}</td>
                    <td className="px-4 py-4 text-xs text-slate-600">{normalizeValue(row.lista_gtin)}</td>
                    <td className="sticky right-0 bg-white px-4 py-4">
                      <div className="flex justify-center">
                        <DropdownMenu>
                          <DropdownMenuTrigger asChild>
                            <Button size="sm" variant="outline" className="gap-1.5">
                              Acoes do codigo
                            </Button>
                          </DropdownMenuTrigger>
                          <DropdownMenuContent align="end" className="w-56">
                            <DropdownMenuItem
                              disabled={statusUpdatingCode === normalizeValue(row.codigo)}
                              onClick={() => void toggleVerified(normalizeValue(row.codigo), normalizeValue(row.lista_descricoes))}
                            >
                              {statusUpdatingCode === normalizeValue(row.codigo)
                                ? "Atualizando verificado..."
                                : verifiedByCodigo.has(normalizeValue(row.codigo))
                                  ? "Desfazer verificado"
                                  : "Verificado"}
                            </DropdownMenuItem>
                            <DropdownMenuItem
                              disabled={autoSeparatingCode === normalizeValue(row.codigo)}
                              onClick={() => void handleAutoSeparateDissimilares(normalizeValue(row.codigo))}
                            >
                              {autoSeparatingCode === normalizeValue(row.codigo) ? "Separando dissimilares..." : "Separar dissimilares"}
                            </DropdownMenuItem>
                            <DropdownMenuItem onClick={() => window.open(`/unificar/${cnpj}/${normalizeValue(row.codigo)}`, "_blank")}>
                              Consolidar
                            </DropdownMenuItem>
                            <DropdownMenuItem onClick={() => window.open(`/desagregar/${cnpj}/${normalizeValue(row.codigo)}`, "_blank")}>
                              Separar
                            </DropdownMenuItem>
                          </DropdownMenuContent>
                        </DropdownMenu>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-500">
        <strong className="text-slate-700">Regra atual:</strong> mesma descricao significa mesmo produto. A revisao residual ficou restrita ao caso em que um codigo foi reutilizado com descricoes diferentes.
      </div>

      <Separator />
    </div>
  );
}
