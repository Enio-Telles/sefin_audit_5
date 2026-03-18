import { Fragment, useDeferredValue, useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useLocation } from "wouter";
import {
  ArrowUpDown,
  Boxes,
  CheckCircle2,
  ChevronLeft,
  Filter,
  FolderOpen,
  Loader2,
  RefreshCw,
  Search,
  ShieldAlert,
  X,
} from "lucide-react";
import { toast } from "sonner";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import {
  applyUnificacaoLote,
  previewUnificacaoLote,
  type BatchRuleId,
  type UnificacaoLoteApplyRequest,
  type UnificacaoLoteProposalItem,
  desfazerProdutoVerificado,
  getParesGruposSimilares,
  getProdutosRevisaoFinal,
  getStatusAnaliseProdutos,
  getVectorizacaoStatus,
  marcarProdutoVerificado,
  readParquet,
  type ProdutoAnaliseStatusItem,
  type ProdutoAnaliseStatusResumo,
} from "@/lib/pythonApi";

type SortDirection = "asc" | "desc" | undefined;

function normalizeText(value: unknown): string {
  return String(value ?? "").trim();
}

function formatCount(value: number | null | undefined): string {
  return new Intl.NumberFormat("pt-BR").format(Number(value || 0));
}

function parseBoolean(value: unknown): boolean {
  if (value === true || value === 1) return true;
  const normalized = normalizeText(value).toLowerCase();
  return normalized === "true" || normalized === "1";
}

function rowKey(row: Record<string, unknown>): string {
  return normalizeText(row.chave_produto);
}

const BATCH_RULE_ORDER: BatchRuleId[] = [
  "R1_HIGH_CONFIDENCE_FULL_FISCAL",
  "R2_NCM_CEST",
  "R3_GTIN_NCM",
  "R6_MANTER_SEPARADO",
];

const BATCH_RULE_BADGE_CLASSNAME: Record<BatchRuleId, string> = {
  R1_HIGH_CONFIDENCE_FULL_FISCAL: "border border-emerald-500/30 bg-emerald-500/12 text-emerald-200",
  R2_NCM_CEST: "border border-blue-500/30 bg-blue-500/12 text-blue-200",
  R3_GTIN_NCM: "border border-cyan-500/30 bg-cyan-500/12 text-cyan-200",
  R6_MANTER_SEPARADO: "border border-amber-500/30 bg-amber-500/12 text-amber-200",
};

type SimilaritySection = {
  id: string;
  label: string;
  helper: string;
  rows: Record<string, unknown>[];
  bestScore: number;
  pairCount: number;
  firstIndex: number;
  blockedCount: number;
  suggestedCount: number;
  singletonBucket?: boolean;
};

function buildSimilaritySections(
  rows: Record<string, unknown>[],
  similarityPairs: Record<string, unknown>[]
): SimilaritySection[] {
  if (rows.length === 0) return [];

  const visibleKeys = new Set(rows.map((row) => rowKey(row)).filter(Boolean));
  const parent = new Map<string, string>();
  const rowIndex = new Map<string, number>();
  rows.forEach((row, index) => rowIndex.set(rowKey(row), index));

  const ensure = (value: string) => {
    if (!parent.has(value)) parent.set(value, value);
  };
  const find = (value: string): string => {
    ensure(value);
    const current = parent.get(value)!;
    if (current === value) return current;
    const root = find(current);
    parent.set(value, root);
    return root;
  };
  const union = (left: string, right: string) => {
    const rootLeft = find(left);
    const rootRight = find(right);
    if (rootLeft !== rootRight) parent.set(rootRight, rootLeft);
  };

  const filteredPairs = similarityPairs
    .map((pair) => ({
      a: normalizeText(pair.chave_produto_a),
      b: normalizeText(pair.chave_produto_b),
      score: Number(pair.score_semantico ?? pair.score_final ?? 0),
      blocked: Boolean(pair.bloquear_uniao),
      suggested: ["UNIR_SUGERIDO", "UNIR_AUTOMATICO_ELEGIVEL"].includes(normalizeText(pair.recomendacao)),
      linkable:
        !Boolean(pair.bloquear_uniao) &&
        (["UNIR_SUGERIDO", "UNIR_AUTOMATICO_ELEGIVEL"].includes(normalizeText(pair.recomendacao)) ||
          Number(pair.score_final ?? pair.score_semantico ?? 0) >= 0.88),
    }))
    .filter((pair) => pair.a && pair.b && visibleKeys.has(pair.a) && visibleKeys.has(pair.b));

  filteredPairs.forEach((pair) => {
    if (pair.linkable) union(pair.a, pair.b);
  });

  const rowsByRoot = new Map<string, Record<string, unknown>[]>();
  rows.forEach((row) => {
    const key = rowKey(row);
    const root = find(key);
    const bucket = rowsByRoot.get(root) || [];
    bucket.push(row);
    rowsByRoot.set(root, bucket);
  });

  const statsByRoot = new Map<string, { bestScore: number; pairCount: number; blockedCount: number; suggestedCount: number }>();
  filteredPairs.forEach((pair) => {
    const root = find(pair.a);
    const current = statsByRoot.get(root) || { bestScore: 0, pairCount: 0, blockedCount: 0, suggestedCount: 0 };
    current.bestScore = Math.max(current.bestScore, pair.score);
    current.pairCount += 1;
    if (pair.blocked) current.blockedCount += 1;
    if (pair.suggested) current.suggestedCount += 1;
    statsByRoot.set(root, current);
  });

  const grouped: SimilaritySection[] = [];
  const singletons: Record<string, unknown>[] = [];

  for (const [root, bucket] of Array.from(rowsByRoot.entries())) {
    const orderedRows = bucket
      .slice()
      .sort(
        (left: Record<string, unknown>, right: Record<string, unknown>) =>
          (rowIndex.get(rowKey(left)) ?? 0) - (rowIndex.get(rowKey(right)) ?? 0)
      );
    if (orderedRows.length <= 1) {
      singletons.push(...orderedRows);
      continue;
    }
    const stats = statsByRoot.get(root) || { bestScore: 0, pairCount: 0, blockedCount: 0, suggestedCount: 0 };
    grouped.push({
      id: root,
      label: `Cluster FAISS ${String(grouped.length + 1).padStart(2, "0")}`,
      helper: `${orderedRows.length} descricoes com proximidade semantica relevante.`,
      rows: orderedRows,
      bestScore: stats.bestScore,
      pairCount: stats.pairCount,
      firstIndex: rowIndex.get(rowKey(orderedRows[0])) ?? Number.MAX_SAFE_INTEGER,
      blockedCount: stats.blockedCount,
      suggestedCount: stats.suggestedCount,
    });
  }

  grouped.sort((left, right) => left.firstIndex - right.firstIndex);
  if (singletons.length > 0) {
    grouped.push({
      id: "faiss-singletons",
      label: "Sem vizinho FAISS relevante",
      helper: `${singletons.length} descricoes permaneceram isoladas com os filtros atuais.`,
      rows: singletons,
      bestScore: 0,
      pairCount: 0,
      firstIndex: rows.length + 1,
      blockedCount: 0,
      suggestedCount: 0,
      singletonBucket: true,
    });
  }
  return grouped;
}

function StatCard({
  accent = false,
  helper,
  label,
  value,
}: {
  accent?: boolean;
  helper: string;
  label: string;
  value: number;
}) {
  return (
    <div className={accent ? "rounded-2xl border border-blue-500/30 bg-blue-500/10 px-4 py-3 shadow-sm" : "rounded-2xl border border-border/70 bg-card/95 px-4 py-3 shadow-sm"}>
      <div className={`text-[10px] font-black uppercase tracking-[0.18em] ${accent ? "text-blue-200" : "text-muted-foreground"}`}>{label}</div>
      <div className={`mt-2 text-2xl font-black ${accent ? "text-white" : "text-foreground"}`}>{formatCount(value)}</div>
      <div className={`mt-1 text-xs leading-5 ${accent ? "text-blue-100/80" : "text-muted-foreground"}`}>{helper}</div>
    </div>
  );
}

type RevisaoFinalProdutosViewProps = {
  embedded?: boolean;
  onBack?: () => void;
};

export function RevisaoFinalProdutosView({
  embedded = false,
  onBack,
}: RevisaoFinalProdutosViewProps = {}) {
  const [, navigate] = useLocation();
  const searchParams = new URLSearchParams(window.location.search);
  const cnpj = normalizeText(searchParams.get("cnpj")).replace(/\D/g, "");
  const initialGroupingMode = normalizeText(searchParams.get("agrupamento")).toLowerCase() === "faiss" ? "faiss" : "flat";

  const [searchInput, setSearchInput] = useState({
    descricao: "",
    ncm: "",
    cest: "",
  });
  const [groupingMode, setGroupingMode] = useState<"flat" | "faiss">(initialGroupingMode);
  const [selectedGroups, setSelectedGroups] = useState<Set<string>>(new Set());
  const [showVerified, setShowVerified] = useState(false);
  const [statusSaving, setStatusSaving] = useState(false);
  const [sortColumn, setSortColumn] = useState<string | undefined>("descricao");
  const [sortDirection, setSortDirection] = useState<SortDirection>("asc");
  const [activeBatchRule, setActiveBatchRule] = useState<BatchRuleId | "ALL">("ALL");
  const [batchApplying, setBatchApplying] = useState<BatchRuleId | "ALL" | null>(null);

  const deferredDescricao = useDeferredValue(searchInput.descricao.trim());
  const deferredNcm = useDeferredValue(searchInput.ncm.trim());
  const deferredCest = useDeferredValue(searchInput.cest.trim());

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
    queryKey: ["produtos-revisao-final-meta", cnpj],
    queryFn: () => getProdutosRevisaoFinal(cnpj),
    enabled: Boolean(cnpj),
    staleTime: 30_000,
    retry: false,
  });

  const filePath = normalizeText(metaQuery.data?.file_path).replace(/\\/g, "/");
  const fileAvailable = Boolean(metaQuery.data?.available && filePath);

  const statusQuery = useQuery({
    queryKey: ["produtos-status-analise", cnpj],
    queryFn: () =>
      getStatusAnaliseProdutos(cnpj).catch(() => ({
        success: true,
        file_path: "",
        data: [] as ProdutoAnaliseStatusItem[],
        resumo: emptyStatusResumo,
      })),
    enabled: Boolean(cnpj),
    staleTime: 20_000,
  });

  const parquetFilters = useMemo(() => {
    const next: Record<string, string> = {};
    if (deferredDescricao) next.lista_descricao = deferredDescricao;
    if (deferredNcm) next.ncm_consenso = deferredNcm;
    if (deferredCest) next.cest_consenso = deferredCest;
    return next;
  }, [deferredCest, deferredDescricao, deferredNcm]);

  const rowsQuery = useQuery({
    queryKey: [
      "produtos-revisao-final-rows",
      filePath,
      deferredDescricao,
      deferredNcm,
      deferredCest,
      sortColumn ?? "",
      sortDirection ?? "",
    ],
    queryFn: () =>
      readParquet({
        file_path: filePath,
        page: 1,
        page_size: 10_000,
        filters: parquetFilters,
        sort_column: sortColumn,
        sort_direction: sortDirection,
      }),
    enabled: fileAvailable,
    retry: false,
    placeholderData: (previousData) => previousData,
  });
  const vectorStatusQuery = useQuery({
    queryKey: ["produtos-revisao-final-vector-status", cnpj],
    queryFn: () => getVectorizacaoStatus(cnpj),
    enabled: Boolean(cnpj && fileAvailable && groupingMode === "faiss"),
    staleTime: 30_000,
    retry: 1,
  });
  const faissCache = vectorStatusQuery.data?.caches?.faiss;
  const faissCacheReady = Boolean(faissCache?.generated_at_utc) && !Boolean(faissCache?.stale);
  const faissStatusLoading = groupingMode === "faiss" && vectorStatusQuery.isLoading;
  const faissStatusRefreshing = groupingMode === "faiss" && vectorStatusQuery.isFetching;
  const effectivePreviewEngine = groupingMode === "faiss" ? (faissCacheReady ? "FAISS" : null) : "DOCUMENTAL";
  const faissMetaQuery = useQuery({
    queryKey: ["produtos-revisao-final-faiss-meta", cnpj, faissCache?.generated_at_utc ?? ""],
    queryFn: () =>
      getParesGruposSimilares(cnpj, "faiss", false, {
        topK: Number(faissCache?.top_k ?? 8),
        minScore: Number(faissCache?.min_semantic_score ?? 0.62),
        page: 1,
        pageSize: 1,
        showAnalyzed: true,
      }),
    enabled: Boolean(cnpj && groupingMode === "faiss" && faissCacheReady),
    staleTime: 30_000,
    retry: 1,
  });
  const faissPairsPath = normalizeText(faissMetaQuery.data?.file_path).replace(/\\/g, "/");
  const faissPairsQuery = useQuery({
    queryKey: ["produtos-revisao-final-faiss-pairs", faissPairsPath],
    queryFn: () =>
      readParquet({
        file_path: faissPairsPath,
        page: 1,
        page_size: 20_000,
        sort_column: "score_final",
        sort_direction: "desc",
      }),
    enabled: Boolean(groupingMode === "faiss" && faissPairsPath),
    retry: 1,
    staleTime: 30_000,
  });
  const batchPreviewQuery = useQuery({
    queryKey: [
      "produtos-revisao-final-batch-preview",
      cnpj,
      deferredDescricao,
      deferredNcm,
      deferredCest,
      showVerified ? "with-verified" : "hide-verified",
      groupingMode,
      faissCache?.generated_at_utc ?? "",
    ],
    queryFn: () =>
      previewUnificacaoLote({
        cnpj,
        source_context: "REVISAO_FINAL",
        filters: {
          descricao_contains: deferredDescricao,
          ncm_contains: deferredNcm,
          cest_contains: deferredCest,
          show_verified: showVerified,
        },
        grouping_mode: groupingMode,
        similarity_source: {
          engine: effectivePreviewEngine || "DOCUMENTAL",
          use_cache: true,
          top_k: Number(faissCache?.top_k ?? 8),
          min_score: Number(faissCache?.min_semantic_score ?? 0.62),
        },
        rule_ids: BATCH_RULE_ORDER,
        options: {
          only_visible: true,
          require_all_pairs_compatible: true,
          max_component_size: 12,
        },
      }),
    enabled: Boolean(cnpj && fileAvailable && effectivePreviewEngine),
    staleTime: 30_000,
    retry: 1,
    placeholderData: (previousData) => previousData,
  });
  const batchPreviewData = batchPreviewQuery.data;
  const batchPreviewPending =
    Boolean(effectivePreviewEngine) &&
    !batchPreviewData &&
    (batchPreviewQuery.isLoading || batchPreviewQuery.isFetching);
  const batchPreviewRefreshing = Boolean(batchPreviewData) && batchPreviewQuery.isFetching;

  const statusRows = statusQuery.data?.data || [];
  const statusResumo = statusQuery.data?.resumo || emptyStatusResumo;
  const verifiedByGroup = useMemo(
    () =>
      new Set(
        statusRows
          .filter((item) => item.tipo_ref === "POR_GRUPO" && item.status_analise === "VERIFICADO_SEM_ACAO")
          .map((item) => normalizeText(item.ref_id))
      ),
    [statusRows]
  );

  const rows = rowsQuery.data?.rows || [];
  const visibleRows = useMemo(
    () => rows.filter((row) => showVerified || !verifiedByGroup.has(rowKey(row))),
    [rows, showVerified, verifiedByGroup]
  );
  const similaritySections = useMemo(() => {
    if (groupingMode !== "faiss") return [];
    return buildSimilaritySections(visibleRows, faissPairsQuery.data?.rows || []);
  }, [faissPairsQuery.data?.rows, groupingMode, visibleRows]);
  const batchSummaryByRule = useMemo(() => {
    const entries = batchPreviewData?.resumo?.by_rule || [];
    return new Map(entries.map((item) => [item.rule_id, item]));
  }, [batchPreviewData?.resumo?.by_rule]);
  const batchProposals = useMemo<UnificacaoLoteProposalItem[]>(() => batchPreviewData?.proposals || [], [batchPreviewData?.proposals]);
  const filteredBatchProposals = useMemo(
    () => batchProposals.filter((item) => activeBatchRule === "ALL" || item.rule_id === activeBatchRule),
    [activeBatchRule, batchProposals]
  );

  const buildBatchRequestPayload = (ruleId: BatchRuleId, proposalIds: string[]): UnificacaoLoteApplyRequest => ({
    cnpj,
    source_context: "REVISAO_FINAL",
    action: ruleId === "R6_MANTER_SEPARADO" ? "MANTER_SEPARADO" : "UNIFICAR",
    rule_id: ruleId,
    proposal_ids: proposalIds,
    filters: {
      descricao_contains: deferredDescricao,
      ncm_contains: deferredNcm,
      cest_contains: deferredCest,
      show_verified: showVerified,
    },
    grouping_mode: groupingMode,
    similarity_source: {
      engine: effectivePreviewEngine || "DOCUMENTAL",
      use_cache: true,
      top_k: Number(faissCache?.top_k ?? 8),
      min_score: Number(faissCache?.min_semantic_score ?? 0.62),
    },
    options: {
      only_visible: true,
      require_all_pairs_compatible: true,
      max_component_size: 12,
    },
  });

  const visibleKeys = useMemo(() => new Set(visibleRows.map((row) => rowKey(row)).filter(Boolean)), [visibleRows]);

  useEffect(() => {
    setSelectedGroups((previous) => {
      const kept = Array.from(previous).filter((item) => visibleKeys.has(item));
      return kept.length === previous.size ? previous : new Set(kept);
    });
  }, [visibleKeys]);

  useEffect(() => {
    const handleConsolidacaoConcluida = (event: MessageEvent) => {
      if (event.origin !== window.location.origin) return;
      if (event.data?.type !== "produto-consolidacao-concluida") return;
      if (normalizeText(event.data?.cnpj) !== cnpj) return;

      setSelectedGroups(new Set());
      void (async () => {
        await metaQuery.refetch();
        await statusQuery.refetch();
        if (fileAvailable) {
          await rowsQuery.refetch();
          await batchPreviewQuery.refetch();
        }
      })();
    };

    window.addEventListener("message", handleConsolidacaoConcluida);
    return () => window.removeEventListener("message", handleConsolidacaoConcluida);
  }, [batchPreviewQuery, cnpj, fileAvailable, metaQuery, rowsQuery, statusQuery]);

  const allVisibleSelected = visibleRows.length > 0 && visibleRows.every((row) => selectedGroups.has(rowKey(row)));
  const selectedRows = visibleRows.filter((row) => selectedGroups.has(rowKey(row)));
  const selectedCount = selectedRows.length;
  const visiblePendingReview = visibleRows.filter((row) => parseBoolean(row.requer_revisao_manual)).length;
  const visibleRowsCount = rowsQuery.isLoading && rows.length === 0 ? Number(metaQuery.data?.summary.total_grupos || 0) : visibleRows.length;
  const visiblePendingCount =
    rowsQuery.isLoading && rows.length === 0 ? Number(metaQuery.data?.summary.grupos_revisao_manual || 0) : visiblePendingReview;

  const refreshAll = async () => {
    try {
      const metaResult = await metaQuery.refetch();
      const statusResult = await statusQuery.refetch();
      const rowsResult = fileAvailable ? await rowsQuery.refetch() : null;
      const vectorResult = groupingMode === "faiss" ? await vectorStatusQuery.refetch() : null;
      const faissMetaResult = groupingMode === "faiss" && faissCacheReady ? await faissMetaQuery.refetch() : null;
      const faissPairsResult = groupingMode === "faiss" && faissPairsPath ? await faissPairsQuery.refetch() : null;
      const batchResult = fileAvailable ? await batchPreviewQuery.refetch() : null;
      if (metaResult.error) throw metaResult.error;
      if (statusResult.error) throw statusResult.error;
      if (rowsResult?.error) throw rowsResult.error;
      if (vectorResult?.error) throw vectorResult.error;
      if (faissMetaResult?.error) throw faissMetaResult.error;
      if (faissPairsResult?.error) throw faissPairsResult.error;
      if (batchResult?.error) throw batchResult.error;
      toast.success("Tabela final atualizada.");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Falha ao atualizar a revisao final.");
    }
  };

  const toggleSelection = (groupId: string) => {
    setSelectedGroups((previous) => {
      const next = new Set(previous);
      if (next.has(groupId)) next.delete(groupId);
      else next.add(groupId);
      return next;
    });
  };

  const toggleAll = () => {
    if (allVisibleSelected) {
      setSelectedGroups(new Set());
      return;
    }
    setSelectedGroups(new Set(visibleRows.map((row) => rowKey(row))));
  };

  const handleSort = (column: string) => {
    if (sortColumn !== column) {
      setSortColumn(column);
      setSortDirection("asc");
      return;
    }
    if (sortDirection === "asc") {
      setSortDirection("desc");
      return;
    }
    if (sortDirection === "desc") {
      setSortColumn(undefined);
      setSortDirection(undefined);
      return;
    }
    setSortDirection("asc");
  };

  const handleAggregateSelected = () => {
    if (selectedCount === 0) {
      toast.error("Selecione ao menos um grupo para consolidar.");
      return;
    }

    const popup = window.open(`/unificar-multi/${cnpj}?codigos=${encodeURIComponent(Array.from(selectedGroups).join(","))}`, "_blank");
    if (!popup) {
      toast.error("Nao foi possivel abrir a janela de consolidacao.");
    }
  };

  const applyBatchRuleSelection = (ruleId: BatchRuleId) => {
    const groups = Array.from(
      new Set(
        batchProposals
          .filter((item) => item.rule_id === ruleId)
          .flatMap((item) => item.chaves_produto)
          .filter((item) => visibleKeys.has(item))
      )
    );
    setSelectedGroups(new Set(groups));
    if (groups.length === 0) {
      toast.error("Nenhum grupo visivel esta elegivel para esta regra.");
      return;
    }
    toast.success(`${formatCount(groups.length)} grupo(s) selecionado(s) a partir do preview de lote.`);
  };

  const applyBatchRuleAggregate = (ruleId: BatchRuleId) => {
    const proposals = batchProposals.filter((item) => item.rule_id === ruleId);
    const groups = Array.from(
      new Set(
        proposals
          .flatMap((item) => item.chaves_produto)
          .filter((item) => visibleKeys.has(item))
      )
    );
    const proposalIds = proposals.map((item) => item.proposal_id);
    if (proposalIds.length === 0) {
      toast.error("Nenhuma proposta elegivel foi encontrada para esta regra.");
      return;
    }
    const actionLabel = ruleId === "R6_MANTER_SEPARADO" ? "manter separados" : "aplicar unificacao em massa";
    const confirmed = window.confirm(
      `Confirma ${actionLabel} para ${formatCount(proposalIds.length)} proposta(s), afetando ${formatCount(groups.length)} grupo(s) visivel(is)?`
    );
    if (!confirmed) return;

    setBatchApplying(ruleId);
    void (async () => {
      try {
        const response = await applyUnificacaoLote(buildBatchRequestPayload(ruleId, proposalIds));
        setSelectedGroups(new Set());
        await Promise.all([metaQuery.refetch(), statusQuery.refetch(), rowsQuery.refetch(), batchPreviewQuery.refetch()]);
        toast.success(
          ruleId === "R6_MANTER_SEPARADO"
            ? `${formatCount(response.applied_count)} proposta(s) marcadas como mantidas separadas.`
            : `${formatCount(response.applied_count)} proposta(s) aplicadas em lote.`
        );
      } catch (error) {
        toast.error(error instanceof Error ? error.message : "Falha ao aplicar o lote.");
      } finally {
        setBatchApplying(null);
      }
    })();
  };

  const applySingleProposal = (proposal: UnificacaoLoteProposalItem) => {
    const visibleProposalGroups = proposal.chaves_produto.filter((item) => visibleKeys.has(item));
    const actionLabel = proposal.rule_id === "R6_MANTER_SEPARADO" ? "manter separados" : "aplicar a unificacao";
    const confirmed = window.confirm(
      `Confirma ${actionLabel} para a proposta ${proposal.proposal_id} (${formatCount(visibleProposalGroups.length)} grupo(s))?`
    );
    if (!confirmed) return;
    setBatchApplying(proposal.rule_id);
    void (async () => {
      try {
        await applyUnificacaoLote(buildBatchRequestPayload(proposal.rule_id, [proposal.proposal_id]));
        setSelectedGroups(new Set());
        await Promise.all([metaQuery.refetch(), statusQuery.refetch(), rowsQuery.refetch(), batchPreviewQuery.refetch()]);
        toast.success("Proposta aplicada com sucesso.");
      } catch (error) {
        toast.error(error instanceof Error ? error.message : "Falha ao aplicar a proposta.");
      } finally {
        setBatchApplying(null);
      }
    })();
  };

  const handleMarkSelectedVerified = async () => {
    if (selectedCount === 0) {
      toast.error("Selecione ao menos um grupo.");
      return;
    }

    setStatusSaving(true);
    try {
      await Promise.all(
        selectedRows.map((row) =>
          marcarProdutoVerificado({
            cnpj,
            tipo_ref: "POR_GRUPO",
            ref_id: rowKey(row),
            descricao_ref: normalizeText(row.descricao || row.lista_descricao),
            contexto_tela: "REVISAO_FINAL",
          })
        )
      );
      setSelectedGroups(new Set());
      toast.success("Grupos marcados como verificados.");
      await Promise.all([statusQuery.refetch(), rowsQuery.refetch()]);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Erro ao marcar grupos como verificados.");
    } finally {
      setStatusSaving(false);
    }
  };

  const handleUnverifySelected = async () => {
    if (selectedCount === 0) {
      toast.error("Selecione ao menos um grupo.");
      return;
    }

    setStatusSaving(true);
    try {
      await Promise.all(
        selectedRows.map((row) =>
          desfazerProdutoVerificado({
            cnpj,
            tipo_ref: "POR_GRUPO",
            ref_id: rowKey(row),
            descricao_ref: normalizeText(row.descricao || row.lista_descricao),
            contexto_tela: "REVISAO_FINAL",
          })
        )
      );
      setSelectedGroups(new Set());
      toast.success("Marcacao de verificado removida.");
      await Promise.all([statusQuery.refetch(), rowsQuery.refetch()]);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Erro ao desfazer a marcacao de verificado.");
    } finally {
      setStatusSaving(false);
    }
  };

  const clearFilters = () => {
    setSearchInput({
      descricao: "",
      ncm: "",
      cest: "",
    });
  };

  const openParquet = () => {
    if (!filePath) return;
    window.open(`/tabelas/view?file_path=${encodeURIComponent(filePath)}`, "_blank");
  };

  const renderDataRow = (row: Record<string, unknown>) => {
    const key = rowKey(row);
    const isSelected = selectedGroups.has(key);
    const requiresReview = parseBoolean(row.requer_revisao_manual);
    const isVerified = verifiedByGroup.has(key);
    const conflitos = normalizeText(row.descricoes_conflitantes)
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);

    return (
      <tr
        key={key}
        className={[
          "cursor-pointer transition-colors hover:bg-blue-500/10",
          requiresReview ? "bg-amber-500/10" : "bg-card/80",
          isSelected ? "bg-blue-500/12" : "",
          isVerified && showVerified ? "text-muted-foreground" : "",
        ]
          .filter(Boolean)
          .join(" ")}
        onClick={() => toggleSelection(key)}
      >
        <td className="px-4 py-3" onClick={(event) => event.stopPropagation()}>
          <Checkbox checked={isSelected} onCheckedChange={() => toggleSelection(key)} />
        </td>
        <td className="px-4 py-3 align-top">
          <div className="space-y-2">
            <div className="font-mono text-xs font-bold text-slate-300">{key}</div>
            <div className="flex flex-wrap gap-1">
              {requiresReview ? (
                <Badge className="border border-amber-500/30 bg-amber-500/15 text-amber-200 hover:bg-amber-500/15">Revisar</Badge>
              ) : (
                <Badge className="border border-emerald-500/30 bg-emerald-500/15 text-emerald-200 hover:bg-emerald-500/15">Estavel</Badge>
              )}
              {isVerified ? (
                <Badge className="border border-border/70 bg-background/70 text-muted-foreground hover:bg-background/70">Verificado</Badge>
              ) : null}
            </div>
          </div>
        </td>
        <td className="max-w-[26rem] px-4 py-3 align-top">
          <div className="space-y-2">
            <div className="font-semibold text-foreground">{normalizeText(row.descricao) || "-"}</div>
            <div className="text-xs leading-5 text-muted-foreground">{normalizeText(row.lista_descricao) || "-"}</div>
            {normalizeText(row.lista_descr_compl) ? (
              <div className="text-xs leading-5 text-slate-300">Compl.: {normalizeText(row.lista_descr_compl)}</div>
            ) : null}
            <div className="flex flex-wrap gap-2 text-[11px]">
              <Badge variant="outline" className="border-border/70 bg-background/70 text-muted-foreground">
                {formatCount(Number(row.qtd_descricoes || 0))} descricao(oes)
              </Badge>
              <Badge variant="outline" className="border-border/70 bg-background/70 text-muted-foreground">
                {formatCount(Number(row.qtd_codigos || 0))} codigo(s)
              </Badge>
            </div>
          </div>
        </td>
        <td className="max-w-[18rem] px-4 py-3 align-top font-mono text-xs text-slate-300">{normalizeText(row.lista_codigos) || "-"}</td>
        <td className="px-4 py-3 align-top font-mono text-xs text-slate-300">{normalizeText(row.ncm_consenso) || "-"}</td>
        <td className="px-4 py-3 align-top font-mono text-xs text-slate-300">{normalizeText(row.cest_consenso) || "-"}</td>
        <td className="px-4 py-3 align-top font-mono text-xs text-slate-300">{normalizeText(row.gtin_consenso) || "-"}</td>
        <td className="max-w-[12rem] px-4 py-3 align-top text-xs text-slate-300">{normalizeText(row.lista_unid) || "-"}</td>
        <td className="px-4 py-3 align-top">
          {conflitos.length > 0 ? (
            <div className="flex flex-wrap gap-1">
              {conflitos.map((item) => (
                <Badge key={item} variant="outline" className="border-amber-500/30 bg-amber-500/12 text-amber-200">
                  {item}
                </Badge>
              ))}
            </div>
          ) : (
            <span className="text-xs text-muted-foreground">Sem conflitos</span>
          )}
        </td>
      </tr>
    );
  };

  if (!cnpj) {
    return (
      <div className="container mx-auto py-6">
        <Empty className="min-h-[60vh] border border-dashed border-border/70 bg-card/95">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <Boxes />
            </EmptyMedia>
            <EmptyTitle>Revisao final sem CNPJ</EmptyTitle>
            <EmptyDescription>
              Abra esta tela a partir do resultado da auditoria ou informe o parametro <code>cnpj</code> na URL.
            </EmptyDescription>
          </EmptyHeader>
          <Button onClick={() => navigate("/auditar")}>Ir para auditoria</Button>
        </Empty>
      </div>
    );
  }

  const metaError = metaQuery.error as (Error & { status?: number }) | null;
  const rowsError = rowsQuery.error as (Error & { status?: number }) | null;

  return (
    <div className={embedded ? "space-y-6" : "container mx-auto space-y-6 py-6"}>
      <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
        <div className="space-y-3">
          {embedded ? (
            <>
              <div className="flex flex-wrap items-center gap-2">
                <h2 className="text-xl font-bold tracking-tight text-foreground">Fila de revisao final</h2>
                <Badge variant="outline" className="border-border/70 bg-card/80 font-mono text-foreground">
                  {cnpj}
                </Badge>
                <Badge variant="outline" className="border-border/70 bg-background/60 text-muted-foreground">
                  {formatCount(visibleRowsCount)} visiveis
                </Badge>
                <Badge className="border border-amber-500/30 bg-amber-500/10 text-amber-200 hover:bg-amber-500/10">
                  {formatCount(visiblePendingCount)} pendentes
                </Badge>
                <Badge className="border border-emerald-500/30 bg-emerald-500/10 text-emerald-200 hover:bg-emerald-500/10">
                  {formatCount(statusResumo.verificados)} verificados
                </Badge>
              </div>
              <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                <span>A tabela abaixo e a fila principal da analise de produtos.</span>
                <Badge className="border border-blue-500/30 bg-blue-500/10 text-blue-200 hover:bg-blue-500/10">
                  Com CEST: {formatCount(metaQuery.data?.summary.grupos_com_cest)}
                </Badge>
                <Badge className="border border-indigo-500/30 bg-indigo-500/10 text-indigo-200 hover:bg-indigo-500/10">
                  Com GTIN: {formatCount(metaQuery.data?.summary.grupos_com_gtin)}
                </Badge>
              </div>
            </>
          ) : (
            <>
              <div className="flex items-center gap-2">
                <Button variant="ghost" size="icon" className="h-8 w-8" onClick={() => (onBack ? onBack() : window.history.back())} aria-label="Voltar" title="Voltar">
                  <ChevronLeft className="h-4 w-4" />
                </Button>
                <h1 className="text-2xl font-bold tracking-tight text-foreground">Revisao final de produtos</h1>
              </div>
              <div className="flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
                <Badge variant="outline" className="border-border/70 bg-card/80 font-mono text-foreground">
                  {cnpj}
                </Badge>
                <span>Tela unica baseada na tabela final ja desagregada, pronta para consolidacao visual.</span>
              </div>
              <div className="flex flex-wrap gap-2">
                <Badge className="border border-blue-500/30 bg-blue-500/10 text-blue-200 hover:bg-blue-500/10">
                  Com CEST: {formatCount(metaQuery.data?.summary.grupos_com_cest)}
                </Badge>
                <Badge className="border border-indigo-500/30 bg-indigo-500/10 text-indigo-200 hover:bg-indigo-500/10">
                  Com GTIN: {formatCount(metaQuery.data?.summary.grupos_com_gtin)}
                </Badge>
              </div>
            </>
          )}
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Button variant={showVerified ? "default" : "outline"} className="gap-2" onClick={() => setShowVerified((previous) => !previous)}>
            <CheckCircle2 className="h-4 w-4" />
            {showVerified ? "Ocultar verificados" : "Mostrar verificados"}
          </Button>
          <Button variant="outline" className="gap-2" onClick={openParquet} disabled={!fileAvailable}>
            <FolderOpen className="h-4 w-4" />
            Abrir parquet
          </Button>
          <Button variant="outline" className="gap-2" onClick={() => void refreshAll()} disabled={metaQuery.isFetching || rowsQuery.isFetching || statusQuery.isFetching}>
            <RefreshCw className={`h-4 w-4 ${metaQuery.isFetching || rowsQuery.isFetching || statusQuery.isFetching ? "animate-spin" : ""}`} />
            Atualizar
          </Button>
          <Button variant="outline" className="gap-2" onClick={() => void handleMarkSelectedVerified()} disabled={selectedCount === 0 || statusSaving}>
            {statusSaving ? <Loader2 className="h-4 w-4 animate-spin" /> : <CheckCircle2 className="h-4 w-4" />}
            Verificar ({selectedCount})
          </Button>
          <Button variant="outline" className="gap-2" onClick={() => void handleUnverifySelected()} disabled={selectedCount === 0 || statusSaving}>
            {statusSaving ? <Loader2 className="h-4 w-4 animate-spin" /> : <X className="h-4 w-4" />}
            Desfazer
          </Button>
          <Button className="gap-2 bg-blue-600 text-white hover:bg-blue-700" onClick={handleAggregateSelected} disabled={selectedCount === 0}>
            <Boxes className="h-4 w-4" />
            Consolidar selecionados ({selectedCount})
          </Button>
        </div>
      </div>

      {embedded ? null : <Separator />}

      {metaQuery.isLoading ? (
        <div className="flex min-h-[42vh] flex-col items-center justify-center gap-3 rounded-3xl border border-border/70 bg-card/95">
          <Loader2 className="h-10 w-10 animate-spin text-muted-foreground" />
          <p className="text-sm text-muted-foreground">Carregando metadados da revisao final...</p>
        </div>
      ) : null}

      {!metaQuery.isLoading && metaError ? (
        <Alert variant="destructive">
          <ShieldAlert className="h-4 w-4" />
          <AlertTitle>Falha ao carregar a tabela final</AlertTitle>
          <AlertDescription>{metaError.message}</AlertDescription>
        </Alert>
      ) : null}

      {!metaQuery.isLoading && !metaError && !fileAvailable ? (
        <Empty className="min-h-[45vh] rounded-3xl border border-dashed border-border/70 bg-card/95">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <Boxes />
            </EmptyMedia>
            <EmptyTitle>Tabela final ainda nao foi gerada</EmptyTitle>
            <EmptyDescription>
              Execute o processamento de produtos para gerar a visao final desagregada e liberar esta revisao unica.
            </EmptyDescription>
          </EmptyHeader>
          <div className="flex flex-wrap items-center justify-center gap-3">
            <Button onClick={() => navigate("/auditar")}>Ir para auditoria</Button>
            <Button variant="outline" onClick={() => void refreshAll()}>
              Atualizar agora
            </Button>
          </div>
        </Empty>
      ) : null}

      {!metaQuery.isLoading && !metaError && fileAvailable ? (
        <>
          {embedded ? null : (
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              <StatCard accent label="Total grupos" value={metaQuery.data?.summary.total_grupos || 0} helper="Linhas disponiveis na tabela final ja desagregada." />
              <StatCard label="Revisao manual" value={metaQuery.data?.summary.grupos_revisao_manual || 0} helper="Grupos que ainda carregam conflitos e merecem triagem visual." />
              <StatCard label="Visiveis agora" value={visibleRowsCount} helper={showVerified ? "Inclui grupos marcados como verificados." : "Oculta grupos ja marcados como verificados."} />
              <StatCard label="Verificados" value={statusResumo.verificados} helper="Grupos revisados sem acao adicional na tela final." />
            </div>
          )}

          <Card className="overflow-hidden border-border/70 bg-card/95 shadow-sm">
            <CardHeader className="border-b border-border/70 bg-gradient-to-r from-slate-950 via-slate-900 to-slate-950">
              <div className="flex flex-col gap-2 xl:flex-row xl:items-end xl:justify-between">
                <div>
                  <CardTitle className="text-lg text-foreground">{embedded ? "Fila e filtros" : "Filtro operacional"}</CardTitle>
                  <CardDescription className="text-muted-foreground">
                    {embedded ? "Use os filtros para reduzir a fila antes de decidir em lote." : "Os filtros aceitam trechos parciais de descricao, NCM e CEST."}
                  </CardDescription>
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <div className="min-w-[200px]">
                    <Select value={groupingMode} onValueChange={(value: "flat" | "faiss") => setGroupingMode(value)}>
                      <SelectTrigger className="h-9 border-border/70 bg-background/70 text-sm">
                        <SelectValue placeholder="Agrupamento visual" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="flat">Tabela plana</SelectItem>
                        <SelectItem value="faiss">Agrupar por similaridade FAISS</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <Badge variant="outline" className="w-fit border-border/70 bg-background/70 text-muted-foreground">
                    Pendentes visiveis: {formatCount(visiblePendingCount)}
                  </Badge>
                </div>
              </div>
            </CardHeader>
            <CardContent className="space-y-4 p-4">
              <div className="grid gap-3 lg:grid-cols-[1.6fr_0.8fr_0.8fr_auto]">
                <div className="space-y-1.5">
                  <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">Trecho da descricao</div>
                  <div className="relative">
                    <Search className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
                    <Input
                      value={searchInput.descricao}
                      onChange={(event) => setSearchInput((previous) => ({ ...previous, descricao: event.target.value }))}
                      className="h-10 border-border/70 bg-background/70 pl-10 text-foreground placeholder:text-muted-foreground"
                      placeholder="Ex: whisky 12 anos, arroz tipo 1..."
                    />
                  </div>
                </div>
                <div className="space-y-1.5">
                  <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">Parte do NCM</div>
                  <div className="relative">
                    <Filter className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
                    <Input
                      value={searchInput.ncm}
                      onChange={(event) => setSearchInput((previous) => ({ ...previous, ncm: event.target.value }))}
                      className="h-10 border-border/70 bg-background/70 pl-10 font-mono text-foreground placeholder:text-muted-foreground"
                      placeholder="Ex: 2203"
                    />
                  </div>
                </div>
                <div className="space-y-1.5">
                  <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">Parte do CEST</div>
                  <div className="relative">
                    <Filter className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
                    <Input
                      value={searchInput.cest}
                      onChange={(event) => setSearchInput((previous) => ({ ...previous, cest: event.target.value }))}
                      className="h-10 border-border/70 bg-background/70 pl-10 font-mono text-foreground placeholder:text-muted-foreground"
                      placeholder="Ex: 0305"
                    />
                  </div>
                </div>
                <div className="flex items-end">
                  <Button variant="ghost" className="h-10 gap-2 text-muted-foreground hover:bg-accent hover:text-foreground" onClick={clearFilters}>
                    <X className="h-4 w-4" />
                    Limpar filtros
                  </Button>
                </div>
              </div>
              <div className="rounded-2xl border border-border/70 bg-accent/30 px-3 py-2 text-xs text-muted-foreground">
                Cada linha desta tabela final representa um grupo ja desagregado. O mesmo codigo nao volta a aparecer em <span className="font-semibold text-foreground">lista_codigos</span> de descricoes diferentes.
              </div>
              {groupingMode === "faiss" ? (
                <div className="rounded-2xl border border-blue-500/20 bg-blue-500/8 px-3 py-2 text-xs text-muted-foreground">
                  {faissStatusLoading || faissStatusRefreshing ? (
                    <>
                      Verificando o cache <span className="font-semibold text-foreground">FAISS</span> para esta base...
                    </>
                  ) : faissCacheReady ? (
                    <>
                      O agrupamento visual usa o cache <span className="font-semibold text-foreground">FAISS</span> ja gerado no painel da auditoria.{" "}
                      {faissPairsQuery.isLoading ? "Lendo pares semanticos..." : `${formatCount(similaritySections.length)} bloco(s) visuais montados com os filtros atuais.`}
                    </>
                  ) : (
                    <>
                      Nenhum cache FAISS pronto para esta base. Gere as sugestoes no card <span className="font-semibold text-foreground">Sistema Atual de Produtos</span> e volte para esta tela.
                    </>
                  )}
                </div>
              ) : null}
            </CardContent>
          </Card>

          <Card className="overflow-hidden border-border/70 bg-card/95 shadow-sm">
            <CardHeader className="border-b border-border/70 bg-slate-950/90">
              <div className="flex flex-col gap-2 xl:flex-row xl:items-center xl:justify-between">
                <div>
                  <CardTitle className="text-lg text-foreground">Lote sugerido</CardTitle>
                  <CardDescription className="text-muted-foreground">
                    Propostas geradas com os filtros atuais para unificar ou manter grupos separados em bloco.
                  </CardDescription>
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant="secondary" className="bg-accent/70 text-muted-foreground">
                    {groupingMode === "faiss" && !faissCacheReady
                      ? faissStatusLoading || faissStatusRefreshing
                        ? "Preparando FAISS..."
                        : "Aguardando cache FAISS"
                      : batchPreviewPending
                        ? "Carregando preview..."
                        : batchPreviewRefreshing
                          ? "Atualizando preview..."
                          : `${formatCount(batchPreviewData?.resumo?.total_proposals || 0)} proposta(s)`}
                  </Badge>
                  <Button
                    variant="outline"
                    size="sm"
                    className="gap-2"
                    onClick={() => void batchPreviewQuery.refetch()}
                    disabled={batchPreviewQuery.isFetching || (groupingMode === "faiss" && !faissCacheReady)}
                  >
                    <RefreshCw className={`h-4 w-4 ${batchPreviewQuery.isFetching ? "animate-spin" : ""}`} />
                    Atualizar lote
                  </Button>
                </div>
              </div>
            </CardHeader>
            <CardContent className="space-y-4 p-4">
              <div className="flex flex-wrap gap-2">
                <Button
                  variant={activeBatchRule === "ALL" ? "default" : "outline"}
                  size="sm"
                  className="gap-2"
                  onClick={() => setActiveBatchRule("ALL")}
                >
                  Todas
                  <Badge variant="secondary" className="bg-background/70 text-muted-foreground">
                    {batchPreviewPending ? "..." : formatCount(batchPreviewData?.resumo?.total_proposals || 0)}
                  </Badge>
                </Button>
                {BATCH_RULE_ORDER.map((ruleId) => {
                  const summary = batchSummaryByRule.get(ruleId);
                  const count = summary?.proposal_count || 0;
                  return (
                    <Button
                      key={ruleId}
                      variant={activeBatchRule === ruleId ? "default" : "outline"}
                      size="sm"
                      className="gap-2"
                      onClick={() => setActiveBatchRule(ruleId)}
                      disabled={batchPreviewPending}
                    >
                      <span>{summary?.button_label || ruleId}</span>
                      <Badge className={BATCH_RULE_BADGE_CLASSNAME[ruleId]}>{batchPreviewPending ? "..." : formatCount(count)}</Badge>
                    </Button>
                  );
                })}
              </div>

              <div className="grid gap-3 xl:grid-cols-[1.2fr_0.8fr]">
                <div className="rounded-2xl border border-border/70 bg-accent/20 p-3">
                  {batchPreviewPending ? (
                    <div className="text-sm text-muted-foreground">Calculando propostas elegiveis para os filtros atuais...</div>
                  ) : (
                    <div className="flex flex-wrap gap-2">
                      {BATCH_RULE_ORDER.map((ruleId) => {
                        const summary = batchSummaryByRule.get(ruleId);
                        return (
                          <Badge key={ruleId} className={BATCH_RULE_BADGE_CLASSNAME[ruleId]}>
                            {(summary?.button_label || ruleId) + ": " + formatCount(summary?.proposal_count || 0)}
                          </Badge>
                        );
                      })}
                    </div>
                  )}
                  <div className="mt-3 text-xs leading-5 text-muted-foreground">
                    Engine do preview:{" "}
                    <span className="font-semibold text-foreground">
                      {groupingMode === "faiss" && !faissCacheReady ? "FAISS (aguardando cache)" : batchPreviewData?.similarity_source?.engine || "DOCUMENTAL"}
                    </span>.{" "}
                    Componentes: <span className="font-semibold text-foreground">{batchPreviewPending ? "..." : formatCount(batchPreviewData?.resumo?.total_components || 0)}</span>.{" "}
                    Pares candidatos: <span className="font-semibold text-foreground">{batchPreviewPending ? "..." : formatCount(batchPreviewData?.resumo?.total_candidate_pairs || 0)}</span>.
                  </div>
                </div>

                <div className="rounded-2xl border border-border/70 bg-accent/20 p-3">
                  <div className="text-[11px] font-black uppercase tracking-[0.18em] text-muted-foreground">Acoes rapidas</div>
                  <div className="mt-3 flex flex-wrap gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      className="gap-2"
                      onClick={() => applyBatchRuleSelection("R1_HIGH_CONFIDENCE_FULL_FISCAL")}
                      disabled={batchPreviewPending || !batchSummaryByRule.get("R1_HIGH_CONFIDENCE_FULL_FISCAL")?.proposal_count}
                    >
                      Selecionar alta confianca
                    </Button>
                    <Button
                      size="sm"
                      className="gap-2 bg-emerald-600 text-white hover:bg-emerald-700"
                      onClick={() => applyBatchRuleAggregate("R1_HIGH_CONFIDENCE_FULL_FISCAL")}
                      disabled={batchPreviewPending || !batchSummaryByRule.get("R1_HIGH_CONFIDENCE_FULL_FISCAL")?.proposal_count || batchApplying !== null || !effectivePreviewEngine}
                    >
                      {batchApplying === "R1_HIGH_CONFIDENCE_FULL_FISCAL" ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                      Consolidar alta confianca
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      className="gap-2"
                      onClick={() => applyBatchRuleSelection("R2_NCM_CEST")}
                      disabled={batchPreviewPending || !batchSummaryByRule.get("R2_NCM_CEST")?.proposal_count}
                    >
                      Selecionar NCM + CEST
                    </Button>
                    <Button
                      size="sm"
                      className="gap-2 bg-blue-600 text-white hover:bg-blue-700"
                      onClick={() => applyBatchRuleAggregate("R2_NCM_CEST")}
                      disabled={batchPreviewPending || !batchSummaryByRule.get("R2_NCM_CEST")?.proposal_count || batchApplying !== null || !effectivePreviewEngine}
                    >
                      {batchApplying === "R2_NCM_CEST" ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                      Consolidar NCM + CEST
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      className="gap-2"
                      onClick={() => applyBatchRuleSelection("R3_GTIN_NCM")}
                      disabled={batchPreviewPending || !batchSummaryByRule.get("R3_GTIN_NCM")?.proposal_count}
                    >
                      Selecionar GTIN + NCM
                    </Button>
                    <Button
                      size="sm"
                      className="gap-2 bg-cyan-600 text-white hover:bg-cyan-700"
                      onClick={() => applyBatchRuleAggregate("R3_GTIN_NCM")}
                      disabled={batchPreviewPending || !batchSummaryByRule.get("R3_GTIN_NCM")?.proposal_count || batchApplying !== null || !effectivePreviewEngine}
                    >
                      {batchApplying === "R3_GTIN_NCM" ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                      Consolidar GTIN + NCM
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      className="gap-2"
                      onClick={() => applyBatchRuleAggregate("R6_MANTER_SEPARADO")}
                      disabled={batchPreviewPending || !batchSummaryByRule.get("R6_MANTER_SEPARADO")?.proposal_count || batchApplying !== null || !effectivePreviewEngine}
                    >
                      {batchApplying === "R6_MANTER_SEPARADO" ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                      Manter separados
                    </Button>
                  </div>
                </div>
              </div>

              {groupingMode === "faiss" && !faissCacheReady ? (
                <div className="rounded-2xl border border-border/70 bg-background/40 px-4 py-6 text-sm text-muted-foreground">
                  {faissStatusLoading || faissStatusRefreshing
                    ? "Aguardando o status do cache FAISS para montar o preview de lote..."
                    : "O preview de lote em FAISS sera habilitado assim que houver cache semantico pronto para esta base."}
                </div>
              ) : batchPreviewPending ? (
                <div className="rounded-2xl border border-border/70 bg-background/40 px-4 py-6 text-sm text-muted-foreground">
                  Gerando preview de lote com as regras conservadoras...
                </div>
              ) : batchPreviewQuery.error ? (
                <Alert variant="destructive">
                  <ShieldAlert className="h-4 w-4" />
                  <AlertTitle>Falha ao carregar o preview de lote</AlertTitle>
                  <AlertDescription>
                    {batchPreviewQuery.error instanceof Error ? batchPreviewQuery.error.message : "Erro ao gerar propostas de lote."}
                  </AlertDescription>
                </Alert>
              ) : filteredBatchProposals.length === 0 ? (
                <div className="rounded-2xl border border-border/70 bg-background/40 px-4 py-6 text-sm text-muted-foreground">
                  Nenhuma proposta elegivel apareceu para a regra selecionada com os filtros atuais.
                </div>
              ) : (
                <div className="space-y-3">
                  {filteredBatchProposals.slice(0, 6).map((proposal) => (
                    <div key={proposal.proposal_id} className="rounded-2xl border border-border/70 bg-background/40 p-3">
                      <div className="flex flex-col gap-2 xl:flex-row xl:items-start xl:justify-between">
                        <div className="space-y-2">
                          <div className="flex flex-wrap items-center gap-2">
                            <Badge className={BATCH_RULE_BADGE_CLASSNAME[proposal.rule_id]}>{proposal.button_label}</Badge>
                            <Badge variant="outline" className="border-border/70 bg-background/70 text-muted-foreground">
                              {proposal.component_size} grupo(s)
                            </Badge>
                            <Badge variant="outline" className="border-border/70 bg-background/70 text-muted-foreground">
                              score {proposal.metrics.score_final_regra.toFixed(3)}
                            </Badge>
                          </div>
                          <div className="text-sm font-semibold text-foreground">{proposal.descricao_canonica_sugerida || proposal.lista_descricoes[0] || proposal.proposal_id}</div>
                          <div className="text-xs leading-5 text-muted-foreground">
                            {proposal.lista_descricoes.join(" | ")}
                          </div>
                          <div className="flex flex-wrap gap-2 text-[11px]">
                            <Badge variant="outline" className="border-border/70 bg-background/70 text-muted-foreground">
                              NCM: {proposal.relation_summary.ncm}
                            </Badge>
                            <Badge variant="outline" className="border-border/70 bg-background/70 text-muted-foreground">
                              CEST: {proposal.relation_summary.cest}
                            </Badge>
                            <Badge variant="outline" className="border-border/70 bg-background/70 text-muted-foreground">
                              GTIN: {proposal.relation_summary.gtin}
                            </Badge>
                          </div>
                        </div>
                        <div className="flex flex-wrap gap-2">
                          <Button
                            variant="outline"
                            size="sm"
                            className="gap-2"
                            onClick={() => setSelectedGroups(new Set(proposal.chaves_produto.filter((item) => visibleKeys.has(item))))}
                          >
                            Selecionar proposta
                          </Button>
                          {proposal.rule_id !== "R6_MANTER_SEPARADO" ? (
                            <Button
                              size="sm"
                              className="gap-2 bg-blue-600 text-white hover:bg-blue-700"
                              onClick={() => applySingleProposal(proposal)}
                              disabled={batchApplying !== null}
                            >
                              {batchApplying === proposal.rule_id ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                              Consolidar proposta
                            </Button>
                          ) : (
                            <Button
                              variant="outline"
                              size="sm"
                              className="gap-2"
                              onClick={() => applySingleProposal(proposal)}
                              disabled={batchApplying !== null}
                            >
                              {batchApplying === proposal.rule_id ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                              Manter separados
                            </Button>
                          )}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>

          {rowsError ? (
            <Alert variant="destructive">
              <ShieldAlert className="h-4 w-4" />
              <AlertTitle>Falha ao carregar as linhas da revisao final</AlertTitle>
              <AlertDescription>{rowsError.message}</AlertDescription>
            </Alert>
          ) : null}

          <Card className="overflow-hidden border-border/70 bg-card/95 shadow-sm">
            <CardHeader className="border-b border-border/70 bg-slate-950/90">
              <div className="flex flex-col gap-2 xl:flex-row xl:items-center xl:justify-between">
                <div>
                  <CardTitle className="text-lg text-foreground">Tabela final para consolidacao visual</CardTitle>
                  <CardDescription className="text-muted-foreground">Selecione grupos para abrir a consolidacao em lote. A tabela responde apenas aos filtros da visao final.</CardDescription>
                </div>
                <div className="flex flex-wrap gap-2">
                  <Badge variant="secondary" className="bg-accent/70 text-muted-foreground">
                    {formatCount(rowsQuery.data?.filtered_rows ?? visibleRowsCount)} linha(s) filtrada(s)
                  </Badge>
                  {groupingMode === "faiss" ? (
                    <Badge variant="secondary" className="bg-indigo-500/15 text-indigo-200">
                      {faissStatusLoading || faissStatusRefreshing ? "FAISS carregando" : faissCacheReady ? `${formatCount(similaritySections.length)} bloco(s) FAISS` : "FAISS sem cache"}
                    </Badge>
                  ) : null}
                  <Badge variant="secondary" className="bg-blue-500/15 text-blue-200">
                    {formatCount(selectedCount)} selecionada(s)
                  </Badge>
                </div>
              </div>
            </CardHeader>
            <CardContent className="p-0">
              {rowsQuery.isLoading && rows.length === 0 ? (
                <div className="flex min-h-[40vh] flex-col items-center justify-center gap-3">
                  <Loader2 className="h-10 w-10 animate-spin text-blue-400" />
                  <p className="text-sm text-muted-foreground">Lendo a tabela final desagregada...</p>
                </div>
              ) : groupingMode === "faiss" && faissCacheReady && faissPairsQuery.isLoading ? (
                <div className="flex min-h-[28vh] flex-col items-center justify-center gap-3">
                  <Loader2 className="h-10 w-10 animate-spin text-indigo-300" />
                  <p className="text-sm text-muted-foreground">Montando blocos de similaridade FAISS...</p>
                </div>
              ) : visibleRows.length === 0 ? (
                <div className="flex min-h-[28vh] flex-col items-center justify-center gap-3 px-6 text-center">
                  <h3 className="text-lg font-semibold text-foreground">Nenhum grupo encontrado</h3>
                  <p className="max-w-2xl text-sm text-muted-foreground">Ajuste os filtros por descricao, NCM ou CEST para recuperar os grupos que voce quer comparar.</p>
                </div>
              ) : (
                <div className="max-h-[72vh] overflow-auto">
                  <table className="w-full border-collapse text-sm">
                    <thead className="sticky top-0 z-10 bg-slate-950/95 backdrop-blur">
                      <tr className="border-b border-border/70">
                        <th className="w-10 px-4 py-3">
                          <Checkbox checked={allVisibleSelected} onCheckedChange={toggleAll} />
                        </th>
                        {[
                          { key: "chave_produto", label: "Grupo" },
                          { key: "descricao", label: "Descricao" },
                          { key: "lista_codigos", label: "Lista codigos" },
                          { key: "ncm_consenso", label: "NCM" },
                          { key: "cest_consenso", label: "CEST" },
                          { key: "gtin_consenso", label: "GTIN" },
                          { key: "lista_unid", label: "Unidades" },
                          { key: "descricoes_conflitantes", label: "Conflitos" },
                        ].map((column) => (
                          <th key={column.key} className="px-4 py-3 text-left text-xs font-black uppercase tracking-[0.16em] text-slate-300">
                            <button type="button" className="flex items-center gap-1 hover:text-blue-300" onClick={() => handleSort(column.key)}>
                              {column.label}
                              <ArrowUpDown className={`h-3 w-3 ${sortColumn === column.key ? "text-blue-300" : "text-slate-500"}`} />
                            </button>
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border/60 bg-card/80">
                      {groupingMode === "faiss" && faissCacheReady
                        ? similaritySections.map((section) => (
                            <Fragment key={section.id}>
                              <tr className="bg-slate-950/90">
                                <td colSpan={9} className="px-4 py-3">
                                  <div className="flex flex-col gap-2 xl:flex-row xl:items-center xl:justify-between">
                                    <div>
                                      <div className="text-xs font-black uppercase tracking-[0.16em] text-slate-200">{section.label}</div>
                                      <div className="text-xs text-muted-foreground">{section.helper}</div>
                                    </div>
                                    <div className="flex flex-wrap gap-2 text-[11px]">
                                      <Badge variant="outline" className="border-indigo-500/30 bg-indigo-500/12 text-indigo-200">
                                        Score max. {section.bestScore ? section.bestScore.toFixed(3) : "-"}
                                      </Badge>
                                      <Badge variant="outline" className="border-border/70 bg-background/60 text-muted-foreground">
                                        {formatCount(section.pairCount)} par(es)
                                      </Badge>
                                      {section.suggestedCount > 0 ? (
                                        <Badge variant="outline" className="border-emerald-500/30 bg-emerald-500/12 text-emerald-200">
                                          {formatCount(section.suggestedCount)} uniao(oes) sugerida(s)
                                        </Badge>
                                      ) : null}
                                      {section.blockedCount > 0 ? (
                                        <Badge variant="outline" className="border-amber-500/30 bg-amber-500/12 text-amber-200">
                                          {formatCount(section.blockedCount)} bloqueio(s)
                                        </Badge>
                                      ) : null}
                                    </div>
                                  </div>
                                </td>
                              </tr>
                              {section.rows.map((row) => renderDataRow(row))}
                            </Fragment>
                          ))
                        : visibleRows.map((row) => renderDataRow(row))}
                    </tbody>
                  </table>
                </div>
              )}
            </CardContent>
          </Card>
        </>
      ) : null}
    </div>
  );
}

export default function RevisaoFinalProdutos() {
  return <RevisaoFinalProdutosView />;
}
