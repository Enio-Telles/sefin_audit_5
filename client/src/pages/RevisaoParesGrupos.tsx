import { useEffect, useMemo, useState } from "react";
import { useLocation } from "wouter";
import {
  ArrowLeftRight,
  CheckCircle2,
  ChevronLeft,
  GitMerge,
  Loader2,
  RefreshCw,
  SplitSquareHorizontal,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import { toast } from "sonner";
import {
  clearVectorizacaoCache,
  desfazerManualDescricoes,
  desfazerProdutoVerificado,
  getParesGruposSimilares,
  getStatusAnaliseProdutos,
  getVectorizacaoStatus,
  marcarProdutoVerificado,
  resolverManualDescricoes,
  type DescricaoManualMapItem,
  type ParesGruposSimilaresItem,
  type ProdutoAnaliseStatusResumo,
  type ProdutoAnaliseStatusItem,
} from "@/lib/pythonApi";

type SelectedGroupRow = {
  chave_produto: string;
  descricao: string;
  ncm_consenso?: string;
  cest_consenso?: string;
  gtin_consenso?: string;
  qtd_codigos?: number;
  descricoes_conflitantes?: string;
};

type PairQuickFilter = "TODOS" | "UNIR_AUTOMATICO" | "BLOQUEIOS" | "REVISAR";
type PairSortKey = "PRIORIDADE" | "SIMILARIDADE" | "RECOMENDACAO";
type SimilarityMode = "lexical" | "semantic" | "hybrid";

function normalizeValue(value: unknown): string {
  return String(value ?? "").trim();
}

function getRowDescription(row: SelectedGroupRow): string {
  return normalizeValue(row.descricao);
}

function getRowKey(row: SelectedGroupRow): string {
  return normalizeValue(row.chave_produto) || getRowDescription(row);
}

function countSelectedCodes(rows: SelectedGroupRow[]): number {
  return rows.reduce((acc, row) => acc + Number(normalizeValue(row.qtd_codigos) || 0), 0);
}

function getPairKey(row: ParesGruposSimilaresItem): string {
  return `${normalizeValue(row.chave_produto_a)}|||${normalizeValue(row.chave_produto_b)}`;
}

function getPairPriority(row: ParesGruposSimilaresItem): number {
  const recomendacao = normalizeValue(row.recomendacao);
  if (Boolean(row.bloquear_uniao) || recomendacao === "BLOQUEAR_UNIAO") return 50;
  if (Boolean(row.uniao_automatica_elegivel) || recomendacao === "UNIR_AUTOMATICO_ELEGIVEL") return 40;
  if (recomendacao === "UNIR_SUGERIDO") return 30;
  if (recomendacao === "SEPARAR_SUGERIDO") return 20;
  return 10;
}

function getPairRowClass(row: ParesGruposSimilaresItem, checked: boolean): string {
  if (checked) return "bg-blue-50/50";
  if (Boolean(row.bloquear_uniao) || normalizeValue(row.recomendacao) === "BLOQUEAR_UNIAO") {
    return "bg-rose-50/50 hover:bg-rose-50";
  }
  if (Boolean(row.uniao_automatica_elegivel) || normalizeValue(row.recomendacao) === "UNIR_AUTOMATICO_ELEGIVEL") {
    return "bg-emerald-50/50 hover:bg-emerald-50";
  }
  return "hover:bg-slate-50/70";
}

function getPairSignals(row: ParesGruposSimilaresItem): string[] {
  const signals: string[] = [];
  const gtinA = normalizeValue(row.gtin_a);
  const gtinB = normalizeValue(row.gtin_b);
  const ncmA = normalizeValue(row.ncm_a);
  const ncmB = normalizeValue(row.ncm_b);
  const cestA = normalizeValue(row.cest_a);
  const cestB = normalizeValue(row.cest_b);

  if (gtinA && gtinB && gtinA === gtinB) signals.push("GTIN igual");
  else if (gtinA && gtinB && gtinA !== gtinB) signals.push("GTIN conflitante");

  if (ncmA && ncmB && ncmA === ncmB && cestA && cestB && cestA === cestB) signals.push("NCM+CEST iguais");
  else if (ncmA && ncmB && ncmA === ncmB) signals.push("NCM igual");
  else if (cestA && cestB && cestA === cestB) signals.push("CEST igual");

  return signals;
}

function summarizePairOverlap(rows: ParesGruposSimilaresItem[]) {
  const adjacency = new Map<string, Set<string>>();
  const degree = new Map<string, number>();
  const pairKeys = rows.map((row) => getPairKey(row));

  rows.forEach((row) => {
    const a = normalizeValue(row.chave_produto_a);
    const b = normalizeValue(row.chave_produto_b);
    if (!adjacency.has(a)) adjacency.set(a, new Set());
    if (!adjacency.has(b)) adjacency.set(b, new Set());
    adjacency.get(a)!.add(b);
    adjacency.get(b)!.add(a);
    degree.set(a, (degree.get(a) ?? 0) + 1);
    degree.set(b, (degree.get(b) ?? 0) + 1);
  });

  const visited = new Set<string>();
  let componentes = 0;
  let componentesSobrepostos = 0;

  adjacency.forEach((_, start) => {
    if (visited.has(start)) return;
    componentes += 1;
    const stack = [start];
    const nodes: string[] = [];
    visited.add(start);
    while (stack.length) {
      const current = stack.pop()!;
      nodes.push(current);
      (adjacency.get(current) ?? new Set()).forEach((next) => {
        if (!visited.has(next)) {
          visited.add(next);
          stack.push(next);
        }
      });
    }
    if (nodes.length > 2) componentesSobrepostos += 1;
  });

  const gruposUnicos = adjacency.size;
  const paresSobrepostos = pairKeys.filter((_, idx) => {
    const row = rows[idx];
    return (degree.get(normalizeValue(row.chave_produto_a)) ?? 0) > 1 || (degree.get(normalizeValue(row.chave_produto_b)) ?? 0) > 1;
  }).length;

  return {
    gruposUnicos,
    regras: rows.length,
    paresSobrepostos,
    componentes,
    componentesSobrepostos,
  };
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

function buildSelectedGroupsFromPairs(rows: ParesGruposSimilaresItem[], selectedPairKeys: string[]): SelectedGroupRow[] {
  const selectedPairs = new Set(selectedPairKeys);
  const groups = new Map<string, SelectedGroupRow>();

  rows
    .filter((row) => selectedPairs.has(getPairKey(row)))
    .forEach((row) => {
      const groupA: SelectedGroupRow = {
        chave_produto: normalizeValue(row.chave_produto_a),
        descricao: normalizeValue(row.descricao_a),
        ncm_consenso: normalizeValue(row.ncm_a),
        cest_consenso: normalizeValue(row.cest_a),
        gtin_consenso: normalizeValue(row.gtin_a),
        qtd_codigos: Number(row.qtd_codigos_a || 0),
        descricoes_conflitantes: normalizeValue(row.conflitos_a),
      };
      const groupB: SelectedGroupRow = {
        chave_produto: normalizeValue(row.chave_produto_b),
        descricao: normalizeValue(row.descricao_b),
        ncm_consenso: normalizeValue(row.ncm_b),
        cest_consenso: normalizeValue(row.cest_b),
        gtin_consenso: normalizeValue(row.gtin_b),
        qtd_codigos: Number(row.qtd_codigos_b || 0),
        descricoes_conflitantes: normalizeValue(row.conflitos_b),
      };
      groups.set(groupA.chave_produto, groupA);
      groups.set(groupB.chave_produto, groupB);
    });

  return Array.from(groups.values()).sort((a, b) => a.descricao.localeCompare(b.descricao));
}

function StatBox({ label, value, highlight = false }: { label: string; value: number; highlight?: boolean }) {
  return (
    <div
      className={
        highlight
          ? "rounded-lg border border-emerald-300 bg-emerald-50 px-4 py-3 shadow-sm"
          : "rounded-lg border border-slate-200 bg-white px-4 py-3"
      }
    >
      <div className={`text-[10px] font-black uppercase tracking-widest ${highlight ? "text-emerald-700" : "text-slate-500"}`}>{label}</div>
      <div className={`mt-1 text-2xl font-black ${highlight ? "text-emerald-900" : "text-slate-900"}`}>{value}</div>
    </div>
  );
}

export default function RevisaoParesGrupos() {
  const [, navigate] = useLocation();
  const searchParams = new URLSearchParams(window.location.search);
  const cnpj = searchParams.get("cnpj") || "";
  const storageKey = `produto-pares-ui:${cnpj}`;

  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [rows, setRows] = useState<ParesGruposSimilaresItem[]>([]);
  const [statusRows, setStatusRows] = useState<ProdutoAnaliseStatusItem[]>([]);
  const [statusResumo, setStatusResumo] = useState<ProdutoAnaliseStatusResumo | null>(null);
  const [selectedPairKeys, setSelectedPairKeys] = useState<string[]>([]);
  const [canonicalKey, setCanonicalKey] = useState<string>("");
  const [search, setSearch] = useState("");
  const [showAnalyzed, setShowAnalyzed] = useState(false);
  const [quickFilter, setQuickFilter] = useState<PairQuickFilter>("TODOS");
  const [sortKey, setSortKey] = useState<PairSortKey>("PRIORIDADE");
  const [similarityMode, setSimilarityMode] = useState<SimilarityMode>("lexical");
  const [semanticTopK, setSemanticTopK] = useState<number>(8);
  const [semanticThreshold, setSemanticThreshold] = useState<number>(0.32);
  const [cacheMeta, setCacheMeta] = useState<{
    metodo?: string;
    engine?: string | null;
    input_base_hash?: string | null;
    generated_at_utc?: string | null;
    modelo_vetorizacao?: string | null;
    top_k?: number | null;
    min_semantic_score?: number | null;
    batch_size?: number | null;
  } | null>(null);
  const [vectorStatus, setVectorStatus] = useState<{
    available: boolean;
    message: string;
    model_name?: string;
    engine?: string | null;
  } | null>(null);
  const [currentBaseHash, setCurrentBaseHash] = useState<string | null>(null);
  const [vectorCaches, setVectorCaches] = useState<{ semantic?: Record<string, unknown>; hybrid?: Record<string, unknown> } | null>(null);
  const [pendingVectorMode, setPendingVectorMode] = useState<SimilarityMode | null>(null);
  const semanticModel = useMemo(
    () => rows.find((row) => normalizeValue(row.modelo_vetorizacao))?.modelo_vetorizacao || "",
    [rows]
  );

  const resetUiState = () => {
    setSearch("");
    setShowAnalyzed(false);
    setQuickFilter("TODOS");
    setSortKey("PRIORIDADE");
    setSimilarityMode("lexical");
    setPendingVectorMode(null);
    window.sessionStorage.removeItem(storageKey);
  };

  useEffect(() => {
    if (!cnpj) return;
    try {
      const raw = window.sessionStorage.getItem(storageKey);
      if (!raw) return;
      const state = JSON.parse(raw) as {
        search?: string;
        showAnalyzed?: boolean;
        quickFilter?: PairQuickFilter;
        sortKey?: PairSortKey;
        similarityMode?: SimilarityMode;
        pendingVectorMode?: SimilarityMode | null;
        semanticTopK?: number;
        semanticThreshold?: number;
        cacheMeta?: {
          metodo?: string;
          engine?: string | null;
          input_base_hash?: string | null;
          generated_at_utc?: string | null;
          modelo_vetorizacao?: string | null;
          top_k?: number | null;
          min_semantic_score?: number | null;
          batch_size?: number | null;
        } | null;
        currentBaseHash?: string | null;
      };
      if (typeof state.search === "string") setSearch(state.search);
      if (typeof state.showAnalyzed === "boolean") setShowAnalyzed(state.showAnalyzed);
      if (state.quickFilter) setQuickFilter(state.quickFilter);
      if (state.sortKey) setSortKey(state.sortKey);
      if (state.similarityMode === "lexical" || state.similarityMode === "semantic" || state.similarityMode === "hybrid") setSimilarityMode(state.similarityMode);
      if (state.pendingVectorMode === "semantic" || state.pendingVectorMode === "hybrid") setPendingVectorMode(state.pendingVectorMode);
      if (typeof state.semanticTopK === "number") setSemanticTopK(state.semanticTopK);
      if (typeof state.semanticThreshold === "number") setSemanticThreshold(state.semanticThreshold);
      if (state.cacheMeta) setCacheMeta(state.cacheMeta);
      if (typeof state.currentBaseHash === "string" || state.currentBaseHash === null) setCurrentBaseHash(state.currentBaseHash ?? null);
    } catch {
      // ignore invalid session state
    }
  }, [cnpj, storageKey]);

  useEffect(() => {
    if (!cnpj) return;
    window.sessionStorage.setItem(
      storageKey,
      JSON.stringify({
        search,
        showAnalyzed,
        quickFilter,
        sortKey,
        similarityMode,
        pendingVectorMode,
        semanticTopK,
        semanticThreshold,
        cacheMeta,
        currentBaseHash,
      })
    );
  }, [cnpj, storageKey, search, showAnalyzed, quickFilter, sortKey, similarityMode, pendingVectorMode, semanticTopK, semanticThreshold, cacheMeta, currentBaseHash]);

  const loadRows = async () => {
    if (!cnpj) {
      setRows([]);
      setStatusRows([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      const [res, statusRes] = await Promise.all([
        getParesGruposSimilares(cnpj, similarityMode, false, { topK: semanticTopK, minSemanticScore: semanticThreshold }),
        getStatusAnaliseProdutos(cnpj).catch(() => ({
          success: true,
          file_path: "",
          data: [] as ProdutoAnaliseStatusItem[],
          resumo: {
            pendentes: 0,
            verificados: 0,
            consolidados: 0,
            separados: 0,
            decididos_entre_grupos: 0,
          } as ProdutoAnaliseStatusResumo,
        })),
      ]);
      if (!res.success && similarityMode === "semantic") {
        toast.error("Modo semantico indisponivel", {
          description: res.message || "Dependencias de vetorizacao indisponiveis neste ambiente.",
        });
        setSimilarityMode("lexical");
        const lexicalRes = await getParesGruposSimilares(cnpj, "lexical", false, { topK: semanticTopK, minSemanticScore: semanticThreshold });
        setRows(lexicalRes.success ? lexicalRes.data : []);
        setCacheMeta(lexicalRes.cache_metadata || null);
      } else if (!res.success && similarityMode === "hybrid") {
        toast.error("Modo hibrido indisponivel", {
          description: res.message || "Dependencias de vetorizacao indisponiveis neste ambiente.",
        });
        setSimilarityMode("lexical");
        const lexicalRes = await getParesGruposSimilares(cnpj, "lexical", false, { topK: semanticTopK, minSemanticScore: semanticThreshold });
        setRows(lexicalRes.success ? lexicalRes.data : []);
        setCacheMeta(lexicalRes.cache_metadata || null);
      } else {
        setRows(res.success ? res.data : []);
        setCacheMeta(res.cache_metadata || null);
      }
      setStatusRows(statusRes.success ? statusRes.data : []);
      setStatusResumo(statusRes.success ? statusRes.resumo : null);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Falha ao carregar pares candidatos.";
      toast.error("Erro ao carregar pares", { description: message });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadRows();
  }, [cnpj, similarityMode, semanticTopK, semanticThreshold]);

  useEffect(() => {
    let active = true;
    if (!cnpj) return;
    getVectorizacaoStatus(cnpj)
      .then((res) => {
        if (active) {
          setVectorStatus(res.status);
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
  }, [cnpj]);

  const handleRecalculateSemantic = async () => {
    if (!cnpj) return;
    setLoading(true);
    try {
      const targetMode: SimilarityMode = similarityMode === "hybrid" ? "hybrid" : "semantic";
      const res = await getParesGruposSimilares(cnpj, targetMode, true, { topK: semanticTopK, minSemanticScore: semanticThreshold });
      if (!res.success) {
        toast.error(targetMode === "hybrid" ? "Modo hibrido indisponivel" : "Modo semantico indisponivel", {
          description: res.message || `Nao foi possivel recalcular os pares ${targetMode === "hybrid" ? "hibridos" : "semanticos"}.`,
        });
        setSimilarityMode("lexical");
        return;
      }
      setRows(res.data || []);
      setCacheMeta(res.cache_metadata || null);
      const statusRes = await getVectorizacaoStatus(cnpj);
      setVectorStatus(statusRes.status);
      setVectorCaches(statusRes.caches || null);
      setCurrentBaseHash(statusRes.current_base_hash || null);
      toast.success(targetMode === "hybrid" ? "Pares hibridos recalculados." : "Pares semanticos recalculados.", {
        description: res.message || "A lista foi atualizada com a vetorizacao mais recente.",
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Falha ao recalcular pares vetorizados.";
      toast.error("Erro ao recalcular modo vetorizado", { description: message });
    } finally {
      setLoading(false);
    }
  };

  const handleClearVectorCache = async () => {
    if (!cnpj) return;
    const targetMode = similarityMode === "hybrid" ? "hybrid" : "semantic";
    if (!window.confirm(`Limpar cache vetorizado do modo ${targetMode}?`)) return;
    setLoading(true);
    try {
      const res = await clearVectorizacaoCache(cnpj, targetMode);
      toast.success("Cache vetorizado removido.", {
        description: `${res.removed.length} arquivo(s) removido(s).`,
      });
      setCacheMeta(null);
      const statusRes = await getVectorizacaoStatus(cnpj);
      setVectorStatus(statusRes.status);
      setVectorCaches(statusRes.caches || null);
      setCurrentBaseHash(statusRes.current_base_hash || null);
      await loadRows();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Falha ao limpar cache vetorizado.";
      toast.error("Erro ao limpar cache", { description: message });
    } finally {
      setLoading(false);
    }
  };

  const statusByGrupo = useMemo(
    () =>
      new Map(
        statusRows
          .filter((item) => normalizeValue(item.tipo_ref) === "POR_GRUPO")
          .map((item) => [normalizeValue(item.ref_id), normalizeValue(item.status_analise)])
      ),
    [statusRows]
  );

  const visibleRows = useMemo(() => {
    const hiddenStatuses = new Set(["VERIFICADO_SEM_ACAO", "UNIDO_ENTRE_GRUPOS", "MANTIDO_SEPARADO"]);
    return showAnalyzed
      ? rows
      : rows.filter((row) => {
          const statusA = statusByGrupo.get(normalizeValue(row.chave_produto_a)) || "";
          const statusB = statusByGrupo.get(normalizeValue(row.chave_produto_b)) || "";
          return !hiddenStatuses.has(statusA) && !hiddenStatuses.has(statusB);
        });
  }, [rows, statusByGrupo, showAnalyzed]);

  const semanticGroupEstimate = useMemo(() => {
    const keys = new Set<string>();
    rows.forEach((row) => {
      keys.add(normalizeValue(row.chave_produto_a));
      keys.add(normalizeValue(row.chave_produto_b));
    });
    const count = keys.size;
    const tier = count <= 150 ? "baixo" : count <= 450 ? "moderado" : "alto";
    return { count, tier };
  }, [rows]);

  const semanticCacheStale = Boolean(vectorCaches?.semantic?.stale);
  const hybridCacheStale = Boolean(vectorCaches?.hybrid?.stale);
  const activeVectorCacheStale =
    similarityMode === "semantic" ? semanticCacheStale : similarityMode === "hybrid" ? hybridCacheStale : false;
  const activeVectorEngine = String(cacheMeta?.engine || vectorStatus?.engine || "").toUpperCase();
  const activeVectorEngineIsFallback = activeVectorEngine === "NUMPY";

  const handleActivateVectorMode = async (mode: SimilarityMode) => {
    if (mode === "lexical") {
      setSimilarityMode("lexical");
      setPendingVectorMode(null);
      return;
    }
    if (!vectorStatus?.available) {
      toast.error(mode === "hybrid" ? "Modo hibrido indisponivel" : "Modo semantico indisponivel", {
        description: vectorStatus?.message || "As dependencias de vetorizacao nao estao disponiveis neste ambiente.",
      });
      return;
    }
    const stale = mode === "semantic" ? semanticCacheStale : hybridCacheStale;
    if (!stale) {
      setSimilarityMode(mode);
      setPendingVectorMode(null);
      return;
    }
    setPendingVectorMode(mode);
    toast.warning("Cache vetorizado desatualizado.", {
      description: `Use "Recalcular e entrar" para ativar o modo ${mode === "hybrid" ? "hibrido" : "semantico"}.`,
    });
  };

  const handleRecalculateAndEnter = async () => {
    const targetMode = pendingVectorMode || (similarityMode === "hybrid" ? "hybrid" : "semantic");
    if (targetMode !== "semantic" && targetMode !== "hybrid") return;
    setSimilarityMode(targetMode);
    await handleRecalculateSemantic();
    setPendingVectorMode(null);
  };

  const quickFilterCounts = useMemo(
    () => ({
      todos: visibleRows.length,
      unirAutomatico: visibleRows.filter((row) => Boolean(row.uniao_automatica_elegivel)).length,
      bloqueios: visibleRows.filter((row) => Boolean(row.bloquear_uniao)).length,
      revisar: visibleRows.filter((row) => normalizeValue(row.recomendacao) === "REVISAR").length,
    }),
    [visibleRows]
  );

  const filteredRows = useMemo(() => {
    const term = search.trim().toUpperCase();
    const quickFilteredRows = visibleRows.filter((row) => {
      if (quickFilter === "UNIR_AUTOMATICO") return Boolean(row.uniao_automatica_elegivel);
      if (quickFilter === "BLOQUEIOS") return Boolean(row.bloquear_uniao);
      if (quickFilter === "REVISAR") return normalizeValue(row.recomendacao) === "REVISAR";
      return true;
    });

    const searchedRows = !term
      ? quickFilteredRows
      : quickFilteredRows.filter((row) => {
          return [
            row.chave_produto_a,
            row.descricao_a,
            row.ncm_a,
            row.cest_a,
            row.gtin_a,
            row.conflitos_a,
            row.chave_produto_b,
            row.descricao_b,
            row.ncm_b,
            row.cest_b,
            row.gtin_b,
            row.conflitos_b,
            row.recomendacao,
            row.motivo_recomendacao,
          ].some((value) => normalizeValue(value).toUpperCase().includes(term));
        });

    return [...searchedRows].sort((a, b) => {
      if (sortKey === "SIMILARIDADE") {
        const scoreDiff = Number(b.score_final || 0) - Number(a.score_final || 0);
        if (scoreDiff !== 0) return scoreDiff;
      }

      if (sortKey === "RECOMENDACAO") {
        const recomendacaoDiff = normalizeValue(a.recomendacao).localeCompare(normalizeValue(b.recomendacao));
        if (recomendacaoDiff !== 0) return recomendacaoDiff;
      }

      const priorityDiff = getPairPriority(b) - getPairPriority(a);
      if (priorityDiff !== 0) return priorityDiff;

      const autoDiff = Number(Boolean(b.uniao_automatica_elegivel)) - Number(Boolean(a.uniao_automatica_elegivel));
      if (autoDiff !== 0) return autoDiff;

      const blockDiff = Number(Boolean(b.bloquear_uniao)) - Number(Boolean(a.bloquear_uniao));
      if (blockDiff !== 0) return blockDiff;

      const finalScoreDiff = Number(b.score_final || 0) - Number(a.score_final || 0);
      if (finalScoreDiff !== 0) return finalScoreDiff;

      const descScoreDiff = Number(b.score_descricao || 0) - Number(a.score_descricao || 0);
      if (descScoreDiff !== 0) return descScoreDiff;

      return normalizeValue(a.descricao_a).localeCompare(normalizeValue(b.descricao_a));
    });
  }, [visibleRows, search, quickFilter, sortKey]);

  const selectedRows = useMemo(() => buildSelectedGroupsFromPairs(filteredRows, selectedPairKeys), [filteredRows, selectedPairKeys]);
  const visibleEligibleOverlap = useMemo(
    () => summarizePairOverlap(filteredRows.filter((row) => Boolean(row.uniao_automatica_elegivel))),
    [filteredRows]
  );

  useEffect(() => {
    const selectedGroupKeys = selectedRows.map((row) => getRowKey(row));
    if (!canonicalKey || !selectedGroupKeys.includes(canonicalKey)) {
      setCanonicalKey(selectedGroupKeys[0] || "");
    }
  }, [selectedRows, canonicalKey]);

  const toggleSelection = (row: ParesGruposSimilaresItem, checked: boolean) => {
    const key = getPairKey(row);
    setSelectedPairKeys((current) => {
      if (checked) {
        return current.includes(key) ? current : [...current, key];
      }
      return current.filter((item) => item !== key);
    });
  };

  const selectVisibleRows = (predicate: (row: ParesGruposSimilaresItem) => boolean) => {
    const keys = filteredRows.filter(predicate).map((row) => getPairKey(row));
    setSelectedPairKeys((current) => Array.from(new Set([...current, ...keys])));
  };

  const clearVisibleSelection = () => {
    const visibleKeys = new Set(filteredRows.map((row) => getPairKey(row)));
    setSelectedPairKeys((current) => current.filter((key) => !visibleKeys.has(key)));
  };

  const buildRowGroups = (row: ParesGruposSimilaresItem): [SelectedGroupRow, SelectedGroupRow] => {
    const groupA: SelectedGroupRow = {
      chave_produto: normalizeValue(row.chave_produto_a),
      descricao: normalizeValue(row.descricao_a),
      ncm_consenso: normalizeValue(row.ncm_a),
      cest_consenso: normalizeValue(row.cest_a),
      gtin_consenso: normalizeValue(row.gtin_a),
      qtd_codigos: Number(row.qtd_codigos_a || 0),
      descricoes_conflitantes: normalizeValue(row.conflitos_a),
    };
    const groupB: SelectedGroupRow = {
      chave_produto: normalizeValue(row.chave_produto_b),
      descricao: normalizeValue(row.descricao_b),
      ncm_consenso: normalizeValue(row.ncm_b),
      cest_consenso: normalizeValue(row.cest_b),
      gtin_consenso: normalizeValue(row.gtin_b),
      qtd_codigos: Number(row.qtd_codigos_b || 0),
      descricoes_conflitantes: normalizeValue(row.conflitos_b),
    };
    return [groupA, groupB];
  };

  const handlePairAction = async (row: ParesGruposSimilaresItem, action: "unir" | "bloquear" | "verificar", canonicalIndex: 0 | 1 = 0) => {
    const [groupA, groupB] = buildRowGroups(row);
    const canonicalGroup = canonicalIndex === 0 ? groupA : groupB;
    const sourceGroup = canonicalIndex === 0 ? groupB : groupA;
    setSelectedPairKeys([getPairKey(row)]);
    setCanonicalKey(getRowKey(canonicalGroup));

    if (action === "verificar") {
      setSaving(true);
      try {
        await Promise.all(
          [groupA, groupB].map((group) =>
            marcarProdutoVerificado({
              cnpj,
              tipo_ref: "POR_GRUPO",
              ref_id: normalizeValue(group.chave_produto),
              descricao_ref: getRowDescription(group),
              contexto_tela: "DECISAO_ENTRE_GRUPOS",
            })
          )
        );
        toast.success("Par marcado como verificado.");
        setSelectedPairKeys([]);
        await loadRows();
      } catch (error) {
        const message = error instanceof Error ? error.message : "Falha ao marcar par como verificado.";
        toast.error("Erro ao marcar verificado", { description: message });
      } finally {
        setSaving(false);
      }
      return;
    }

    const regras: DescricaoManualMapItem[] =
      action === "unir"
        ? [
            {
              tipo_regra: "UNIR_GRUPOS",
              descricao_origem: getRowDescription(sourceGroup),
              descricao_destino: getRowDescription(canonicalGroup),
              descricao_par: getRowDescription(canonicalGroup),
              chave_grupo_a: normalizeValue(sourceGroup.chave_produto),
              chave_grupo_b: normalizeValue(canonicalGroup.chave_produto),
              acao_manual: "AGREGAR",
            },
          ]
        : [
            {
              tipo_regra: "MANTER_SEPARADO",
              descricao_origem: getRowDescription(groupA),
              descricao_destino: "",
              descricao_par: getRowDescription(groupB),
              chave_grupo_a: normalizeValue(groupA.chave_produto),
              chave_grupo_b: normalizeValue(groupB.chave_produto),
              acao_manual: "AGREGAR",
            },
          ];

    const confirmMessage =
      action === "unir"
        ? `Unir diretamente ${getRowDescription(groupA)} com ${getRowDescription(groupB)} mantendo ${getRowDescription(canonicalGroup)} como canonico?`
        : `Bloquear uniao entre ${getRowDescription(groupA)} e ${getRowDescription(groupB)}?`;

    if (!window.confirm(confirmMessage)) return;

    setSaving(true);
    try {
      const res = await resolverManualDescricoes(cnpj, regras);
      toast.success(action === "unir" ? "Par unido." : "Bloqueio registrado.", {
        description: `${res.qtd_regras} regra(s) gravadas.`,
      });
      setSelectedPairKeys([]);
      await loadRows();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Falha ao gravar regra do par.";
      toast.error("Erro ao gravar regra", { description: message });
    } finally {
      setSaving(false);
    }
  };

  const buildUnirRegras = (): DescricaoManualMapItem[] => {
    const canonicalRow = selectedRows.find((row) => getRowKey(row) === canonicalKey);
    const descricaoDestino = canonicalRow ? getRowDescription(canonicalRow) : "";
    const chaveDestino = canonicalRow ? normalizeValue(canonicalRow.chave_produto) : "";

    if (!descricaoDestino) return [];

    return selectedRows
      .filter((row) => getRowKey(row) !== canonicalKey)
      .map((row) => ({
        tipo_regra: "UNIR_GRUPOS",
        descricao_origem: getRowDescription(row),
        descricao_destino: descricaoDestino,
        descricao_par: descricaoDestino,
        chave_grupo_a: normalizeValue(row.chave_produto),
        chave_grupo_b: chaveDestino,
        acao_manual: "AGREGAR",
      }))
      .filter((rule) => rule.descricao_origem && rule.descricao_destino);
  };

  const buildSepararRegras = (): DescricaoManualMapItem[] => {
    const regras: DescricaoManualMapItem[] = [];
    for (let i = 0; i < selectedRows.length; i += 1) {
      for (let j = i + 1; j < selectedRows.length; j += 1) {
        const rowA = selectedRows[i];
        const rowB = selectedRows[j];
        const descricaoA = getRowDescription(rowA);
        const descricaoB = getRowDescription(rowB);
        if (!descricaoA || !descricaoB || descricaoA === descricaoB) continue;
        regras.push({
          tipo_regra: "MANTER_SEPARADO",
          descricao_origem: descricaoA,
          descricao_destino: "",
          descricao_par: descricaoB,
          chave_grupo_a: normalizeValue(rowA.chave_produto),
          chave_grupo_b: normalizeValue(rowB.chave_produto),
          acao_manual: "AGREGAR",
        });
      }
    }
    return regras;
  };

  const submitRules = async (mode: "unir" | "separar") => {
    if (selectedRows.length < 2) {
      toast.error("Selecione pelo menos dois grupos.");
      return;
    }

    const regras = mode === "unir" ? buildUnirRegras() : buildSepararRegras();
    if (regras.length === 0) {
      toast.error("Nao ha regras validas para gravar.");
      return;
    }

    const confirmMessage =
      mode === "unir"
        ? `Unir ${selectedRows.length} grupos na descricao canonica selecionada?`
        : `Registrar ${regras.length} regra(s) para manter ${selectedRows.length} grupos separados?`;

    if (!window.confirm(confirmMessage)) return;

    setSaving(true);
    try {
      const res = await resolverManualDescricoes(cnpj, regras);
      toast.success(mode === "unir" ? "Regras de uniao aplicadas." : "Regras de separacao registradas.", {
        description: `${res.qtd_regras} regra(s) gravadas.`,
      });
      setSelectedPairKeys([]);
      setCanonicalKey("");
      await loadRows();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Falha ao gravar mapa manual de descricoes.";
      toast.error("Erro ao gravar regras", { description: message });
    } finally {
      setSaving(false);
    }
  };

  const handleUndoRules = async () => {
    if (selectedRows.length < 2) {
      toast.error("Selecione pelo menos dois grupos.");
      return;
    }

    const descricoes = selectedRows.map((row) => getRowDescription(row)).filter(Boolean);
    if (descricoes.length < 2) {
      toast.error("Nao ha descricoes suficientes para desfazer regras.");
      return;
    }

    if (!window.confirm(`Remover regras manuais de descricao entre ${selectedRows.length} grupos selecionados?`)) {
      return;
    }

    setSaving(true);
    try {
      const res = await desfazerManualDescricoes(cnpj, descricoes);
      toast.success("Regras removidas.", {
        description: `${res.qtd_regras_removidas} regra(s) removida(s).`,
      });
      setSelectedPairKeys([]);
      setCanonicalKey("");
      await loadRows();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Falha ao remover regras manuais de descricoes.";
      toast.error("Erro ao desfazer regras", { description: message });
    } finally {
      setSaving(false);
    }
  };

  const handleMarkVerified = async () => {
    if (selectedRows.length < 1) {
      toast.error("Selecione ao menos um grupo.");
      return;
    }

    setSaving(true);
    try {
      await Promise.all(
        selectedRows.map((row) =>
          marcarProdutoVerificado({
            cnpj,
            tipo_ref: "POR_GRUPO",
            ref_id: normalizeValue(row.chave_produto),
            descricao_ref: getRowDescription(row),
            contexto_tela: "DECISAO_ENTRE_GRUPOS",
          })
        )
      );
      toast.success("Grupos marcados como verificados.");
      setSelectedPairKeys([]);
      await loadRows();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Falha ao marcar grupos como verificados.";
      toast.error("Erro ao marcar verificado", { description: message });
    } finally {
      setSaving(false);
    }
  };

  const handleUndoVerified = async () => {
    if (selectedRows.length < 1) {
      toast.error("Selecione ao menos um grupo.");
      return;
    }

    setSaving(true);
    try {
      await Promise.all(
        selectedRows.map((row) =>
          desfazerProdutoVerificado({
            cnpj,
            tipo_ref: "POR_GRUPO",
            ref_id: normalizeValue(row.chave_produto),
            descricao_ref: getRowDescription(row),
            contexto_tela: "DECISAO_ENTRE_GRUPOS",
          })
        )
      );
      toast.success("Marcacao de verificado removida.");
      setSelectedPairKeys([]);
      await loadRows();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Falha ao desfazer verificado.";
      toast.error("Erro ao desfazer verificado", { description: message });
    } finally {
      setSaving(false);
    }
  };

  const handleExportVisiblePairs = () => {
    const exportRows = filteredRows.map((row) => ({
      chave_produto_a: normalizeValue(row.chave_produto_a),
      descricao_a: normalizeValue(row.descricao_a),
      ncm_a: normalizeValue(row.ncm_a),
      cest_a: normalizeValue(row.cest_a),
      gtin_a: normalizeValue(row.gtin_a),
      chave_produto_b: normalizeValue(row.chave_produto_b),
      descricao_b: normalizeValue(row.descricao_b),
      ncm_b: normalizeValue(row.ncm_b),
      cest_b: normalizeValue(row.cest_b),
      gtin_b: normalizeValue(row.gtin_b),
      score_final: Number(row.score_final || 0),
      score_descricao: Number(row.score_descricao || 0),
      score_semantico: Number(row.score_semantico || 0),
      recomendacao: normalizeValue(row.recomendacao),
      motivo_recomendacao: normalizeValue(row.motivo_recomendacao),
      origem_par_hibrido: normalizeValue(row.origem_par_hibrido),
      uniao_automatica_elegivel: Boolean(row.uniao_automatica_elegivel),
      bloquear_uniao: Boolean(row.bloquear_uniao),
    }));
    downloadCsv(`pares_grupos_visiveis_${cnpj}.csv`, exportRows);
  };

  const handleExportCurrentFilter = () => {
    const exportRows = filteredRows.map((row) => ({
      filtro_rapido: quickFilter,
      chave_produto_a: normalizeValue(row.chave_produto_a),
      descricao_a: normalizeValue(row.descricao_a),
      chave_produto_b: normalizeValue(row.chave_produto_b),
      descricao_b: normalizeValue(row.descricao_b),
      score_final: Number(row.score_final || 0),
      score_descricao: Number(row.score_descricao || 0),
      score_semantico: Number(row.score_semantico || 0),
      recomendacao: normalizeValue(row.recomendacao),
      motivo_recomendacao: normalizeValue(row.motivo_recomendacao),
      origem_par_hibrido: normalizeValue(row.origem_par_hibrido),
    }));
    downloadCsv(`pares_grupos_filtrados_${cnpj}.csv`, exportRows);
  };

  const handleMarkVisibleVerified = async () => {
    const groups = buildSelectedGroupsFromPairs(
      filteredRows,
      filteredRows.map((row) => getPairKey(row))
    );
    if (groups.length === 0) {
      toast.error("Nao ha grupos visiveis para marcar.");
      return;
    }
    if (!window.confirm(`Marcar ${groups.length} grupo(s) visiveis como verificados?`)) return;

    setSaving(true);
    try {
      await Promise.all(
        groups.map((group) =>
          marcarProdutoVerificado({
            cnpj,
            tipo_ref: "POR_GRUPO",
            ref_id: normalizeValue(group.chave_produto),
            descricao_ref: getRowDescription(group),
            contexto_tela: "DECISAO_ENTRE_GRUPOS",
          })
        )
      );
      toast.success("Grupos visiveis marcados como verificados.");
      setSelectedPairKeys([]);
      await loadRows();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Falha ao marcar grupos visiveis como verificados.";
      toast.error("Erro ao marcar verificado", { description: message });
    } finally {
      setSaving(false);
    }
  };

  const handleBlockVisiblePairs = async () => {
    if (filteredRows.length === 0) {
      toast.error("Nao ha pares visiveis para bloquear.");
      return;
    }
    if (!window.confirm(`Bloquear uniao para ${filteredRows.length} par(es) visiveis?`)) return;

    const regras: DescricaoManualMapItem[] = filteredRows.map((row) => ({
      tipo_regra: "MANTER_SEPARADO",
      descricao_origem: normalizeValue(row.descricao_a),
      descricao_destino: "",
      descricao_par: normalizeValue(row.descricao_b),
      chave_grupo_a: normalizeValue(row.chave_produto_a),
      chave_grupo_b: normalizeValue(row.chave_produto_b),
      acao_manual: "AGREGAR",
    }));

    setSaving(true);
    try {
      const res = await resolverManualDescricoes(cnpj, regras);
      toast.success("Bloqueios aplicados nos pares visiveis.", {
        description: `${res.qtd_regras} regra(s) gravadas.`,
      });
      setSelectedPairKeys([]);
      await loadRows();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Falha ao bloquear pares visiveis.";
      toast.error("Erro ao bloquear pares", { description: message });
    } finally {
      setSaving(false);
    }
  };

  const handleUnirVisibleEligible = async () => {
    const targetRows = filteredRows.filter((row) => Boolean(row.uniao_automatica_elegivel));
    if (targetRows.length === 0) {
      toast.error("Nao ha pares elegiveis para uniao automatica na lista visivel.");
      return;
    }
    const overlap = summarizePairOverlap(targetRows);
    if (
      !window.confirm(
        [
          `Unir ${targetRows.length} par(es) visiveis elegiveis mantendo o Grupo A como canonico em cada par?`,
          "",
          `Grupos unicos afetados: ${overlap.gruposUnicos}`,
          `Regras a gravar: ${overlap.regras}`,
          `Pares sobrepostos: ${overlap.paresSobrepostos}`,
          `Componentes encadeados: ${overlap.componentesSobrepostos}/${overlap.componentes}`,
        ].join("\n")
      )
    ) {
      return;
    }

    const regras: DescricaoManualMapItem[] = targetRows.map((row) => ({
      tipo_regra: "UNIR_GRUPOS",
      descricao_origem: normalizeValue(row.descricao_b),
      descricao_destino: normalizeValue(row.descricao_a),
      descricao_par: normalizeValue(row.descricao_a),
      chave_grupo_a: normalizeValue(row.chave_produto_b),
      chave_grupo_b: normalizeValue(row.chave_produto_a),
      acao_manual: "AGREGAR",
    }));

    setSaving(true);
    try {
      const res = await resolverManualDescricoes(cnpj, regras);
      toast.success("Unioes aplicadas nos pares elegiveis visiveis.", {
        description: `${res.qtd_regras} regra(s) gravadas.`,
      });
      setSelectedPairKeys([]);
      await loadRows();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Falha ao unir pares visiveis elegiveis.";
      toast.error("Erro ao unir pares", { description: message });
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-5 px-4 py-5">
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div className="space-y-2">
          <Button variant="ghost" size="sm" className="-ml-3 w-fit gap-2" onClick={() => window.history.back()}>
            <ChevronLeft className="h-4 w-4" />
            Voltar
          </Button>
          <div>
            <h1 className="text-xl font-black text-slate-900">Decisao entre grupos</h1>
            <p className="mt-1 text-sm text-slate-500">CNPJ {cnpj} | Pares candidatos para uniao manual ou bloqueio de convergencia.</p>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Button variant="outline" className="gap-2" onClick={resetUiState}>
            Restaurar padrao
          </Button>
          <Button
            variant={similarityMode === "lexical" ? "default" : "outline"}
            className="gap-2"
            onClick={() => void handleActivateVectorMode("lexical")}
          >
            Modo lexical
          </Button>
          <Button
            variant={similarityMode === "semantic" ? "default" : "outline"}
            className="gap-2"
            onClick={() => void handleActivateVectorMode("semantic")}
          >
            {`Modo semantico${semanticCacheStale ? " (cache antigo)" : ""}`}
          </Button>
          <Button
            variant={similarityMode === "hybrid" ? "default" : "outline"}
            className="gap-2"
            onClick={() => void handleActivateVectorMode("hybrid")}
          >
            {`Modo hibrido${hybridCacheStale ? " (cache antigo)" : ""}`}
          </Button>
          {pendingVectorMode ? (
            <Button
              variant="default"
              className="gap-2"
              onClick={() => void handleRecalculateAndEnter()}
              disabled={loading}
            >
              <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
              {`Recalcular e entrar (${pendingVectorMode === "hybrid" ? "hibrido" : "semantico"})`}
            </Button>
          ) : null}
          <Button
            variant="outline"
            className="gap-2"
            onClick={() => void handleRecalculateSemantic()}
            disabled={(similarityMode !== "semantic" && similarityMode !== "hybrid") || loading}
          >
            <RefreshCw className={`h-4 w-4 ${loading && (similarityMode === "semantic" || similarityMode === "hybrid") ? "animate-spin" : ""}`} />
            Recalcular {similarityMode === "hybrid" ? "hibrido" : "semantico"}
          </Button>
          <Button
            variant="outline"
            className="gap-2"
            onClick={() => void handleClearVectorCache()}
            disabled={(similarityMode !== "semantic" && similarityMode !== "hybrid") || loading}
          >
            Limpar cache vetorizado
          </Button>
          <Button variant={showAnalyzed ? "default" : "outline"} className="gap-2" onClick={() => setShowAnalyzed((prev) => !prev)}>
            <CheckCircle2 className="h-4 w-4" />
            {showAnalyzed ? "Ocultar analisados" : "Mostrar analisados"}
          </Button>
          <Button variant="outline" className="gap-2" onClick={() => void loadRows()}>
            <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
            Atualizar
          </Button>
          <Button variant="outline" className="gap-2" onClick={() => navigate(`/revisao-manual?cnpj=${encodeURIComponent(cnpj)}`)}>
            <ArrowLeftRight className="h-4 w-4" />
            Voltar para revisao residual
          </Button>
        </div>
      </div>

      {similarityMode === "semantic" || similarityMode === "hybrid" ? (
        <div className="flex flex-wrap items-end gap-3 rounded-lg border border-slate-200 bg-white px-4 py-3">
          <div className="min-w-[120px]">
            <div className="mb-1 text-[11px] font-semibold text-slate-500">Top K</div>
            <select
              className="h-9 w-full rounded-md border border-slate-300 bg-white px-2 text-sm"
              value={semanticTopK}
              onChange={(event) => setSemanticTopK(Number(event.target.value))}
            >
              {[4, 6, 8, 12, 16].map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </div>
          <div className="min-w-[160px]">
            <div className="mb-1 text-[11px] font-semibold text-slate-500">Limiar semantico minimo</div>
            <input
              type="number"
              min={0.05}
              max={0.95}
              step={0.01}
              className="h-9 w-full rounded-md border border-slate-300 bg-white px-2 text-sm"
              value={semanticThreshold}
              onChange={(event) => setSemanticThreshold(Number(event.target.value || 0.32))}
            />
          </div>
          <div className="rounded-md bg-slate-50 px-3 py-2 text-xs text-slate-600">
            Custo estimado: <strong className="text-slate-800">{semanticGroupEstimate.tier}</strong> para {semanticGroupEstimate.count} grupos candidatos.
          </div>
          {(similarityMode === "semantic" || similarityMode === "hybrid") && activeVectorEngine ? (
            <div className="rounded-md bg-indigo-50 px-3 py-2 text-xs text-indigo-700">
              Engine ativa: <strong>{activeVectorEngine}</strong>{activeVectorEngineIsFallback ? " (fallback)" : ""}
            </div>
          ) : null}
        </div>
      ) : null}

      <div className="grid gap-3 md:grid-cols-3">
        <StatBox label="Pares candidatos" value={filteredRows.length} />
        <StatBox label="Selecionados" value={selectedRows.length} />
        <StatBox label="Codigos envolvidos" value={countSelectedCodes(selectedRows)} />
      </div>

      <div className="grid gap-3 md:grid-cols-5">
        <StatBox label="Pendentes" value={statusResumo?.pendentes ?? 0} />
        <StatBox label="Verificados" value={statusResumo?.verificados ?? 0} />
        <StatBox label="Consolidados" value={statusResumo?.consolidados ?? 0} />
        <StatBox label="Separados" value={statusResumo?.separados ?? 0} />
        <StatBox label="Decididos entre grupos" value={statusResumo?.decididos_entre_grupos ?? 0} highlight />
      </div>

      {visibleEligibleOverlap.componentesSobrepostos > 0 ? (
        <div className="rounded-lg border border-amber-300 bg-amber-50 px-4 py-3 text-sm text-amber-900">
          <strong>Encadeamento detectado no subconjunto visivel.</strong>{" "}
          {visibleEligibleOverlap.paresSobrepostos} par(es) elegiveis se sobrepoem, envolvendo {visibleEligibleOverlap.gruposUnicos} grupo(s) unico(s) em{" "}
          {visibleEligibleOverlap.componentesSobrepostos} componente(s) encadeado(s).
        </div>
      ) : null}

      {similarityMode === "semantic" || similarityMode === "hybrid" ? (
        <div className="rounded-lg border border-indigo-200 bg-indigo-50 px-4 py-3 text-sm text-indigo-900">
          <strong>{similarityMode === "hybrid" ? "Modo hibrido ativo." : "Modo semantico ativo."}</strong> A lista combina score lexical, afinidade fiscal e vetorizacao semantica.
          {vectorStatus ? <span className="ml-1 text-indigo-700">Disponibilidade: {vectorStatus.available ? "ok" : "indisponivel"}.</span> : null}
          {semanticModel || cacheMeta?.modelo_vetorizacao ? (
            <span className="ml-1 text-indigo-700">Modelo: {semanticModel || cacheMeta?.modelo_vetorizacao}.</span>
          ) : null}
          {cacheMeta?.engine || vectorStatus?.engine ? <span className="ml-1 text-indigo-700">Engine: {String(cacheMeta?.engine || vectorStatus?.engine).toUpperCase()}.</span> : null}
          {activeVectorEngineIsFallback ? <span className="ml-1 text-indigo-700">Execucao em fallback NUMPY.</span> : null}
          {cacheMeta?.top_k ? <span className="ml-1 text-indigo-700">Top K: {cacheMeta.top_k}.</span> : null}
          {cacheMeta?.min_semantic_score != null ? <span className="ml-1 text-indigo-700">Limiar: {cacheMeta.min_semantic_score}.</span> : null}
          {cacheMeta?.generated_at_utc ? (
            <span className="ml-1 text-indigo-700">Ultimo calculo: {new Date(cacheMeta.generated_at_utc).toLocaleString("pt-BR")}.</span>
          ) : null}
          {cacheMeta?.input_base_hash ? <span className="ml-1 text-indigo-700">Base: {String(cacheMeta.input_base_hash).slice(0, 10)}...</span> : null}
          {currentBaseHash ? <span className="ml-1 text-indigo-700">Base atual: {String(currentBaseHash).slice(0, 10)}...</span> : null}
          {vectorStatus?.message ? <span className="ml-1 text-indigo-700">{vectorStatus.message}</span> : null}
        </div>
      ) : null}

      {(similarityMode === "semantic" || similarityMode === "hybrid") && activeVectorCacheStale ? (
        <div className="rounded-lg border border-amber-300 bg-amber-50 px-4 py-3 text-sm text-amber-900">
          <strong>Cache vetorizado desatualizado.</strong> O modo {similarityMode === "hybrid" ? "hibrido" : "semantico"} foi gerado sobre uma base diferente da atual.
          {currentBaseHash ? <span className="ml-1">Base atual: {String(currentBaseHash).slice(0, 10)}...</span> : null}
          <span className="ml-1">Recalcule antes de tomar decisoes com base nesses pares.</span>
        </div>
      ) : null}

      {pendingVectorMode ? (
        <div className="rounded-lg border border-sky-300 bg-sky-50 px-4 py-3 text-sm text-sky-900">
          <strong>Modo vetorizado pendente.</strong> O modo {pendingVectorMode === "hybrid" ? "hibrido" : "semantico"} sera ativado somente apos o recalculo contra a base atual.
        </div>
      ) : null}

      <div className="grid gap-5 xl:grid-cols-[1.55fr_0.85fr]">
        <div className="overflow-hidden rounded-xl border border-slate-200 bg-white">
          <div className="border-b bg-slate-50 px-4 py-3">
            <div className="text-sm font-black text-slate-900">Pares candidatos</div>
            <div className="mt-1 text-xs text-slate-500">A lista mostra pares de grupos com similaridade textual e/ou fiscal suficiente para decisão.</div>
            <div className="mt-3 flex flex-wrap gap-2">
              <Button size="sm" variant={quickFilter === "TODOS" ? "default" : "outline"} onClick={() => setQuickFilter("TODOS")}>
                Todos ({quickFilterCounts.todos})
              </Button>
              <Button size="sm" variant={quickFilter === "UNIR_AUTOMATICO" ? "default" : "outline"} onClick={() => setQuickFilter("UNIR_AUTOMATICO")}>
                Unir automaticamente ({quickFilterCounts.unirAutomatico})
              </Button>
              <Button size="sm" variant={quickFilter === "BLOQUEIOS" ? "default" : "outline"} onClick={() => setQuickFilter("BLOQUEIOS")}>
                Bloqueios ({quickFilterCounts.bloqueios})
              </Button>
              <Button size="sm" variant={quickFilter === "REVISAR" ? "default" : "outline"} onClick={() => setQuickFilter("REVISAR")}>
                Revisar ({quickFilterCounts.revisar})
              </Button>
            </div>
            <div className="mt-2 flex flex-wrap gap-2">
              <Button size="sm" variant="outline" onClick={() => selectVisibleRows((row) => Boolean(row.uniao_automatica_elegivel))}>
                Selecionar unioes automaticas visiveis
              </Button>
              <Button size="sm" variant="outline" onClick={() => selectVisibleRows((row) => Boolean(row.bloquear_uniao))}>
                Selecionar bloqueios visiveis
              </Button>
              <Button size="sm" variant="ghost" onClick={clearVisibleSelection}>
                Limpar selecao visivel
              </Button>
              <Button size="sm" variant="ghost" onClick={() => void handleMarkVisibleVerified()} disabled={saving || filteredRows.length === 0}>
                Marcar grupos visiveis como verificados
              </Button>
              <Button size="sm" variant="ghost" onClick={() => void handleBlockVisiblePairs()} disabled={saving || filteredRows.length === 0}>
                Bloquear pares visiveis
              </Button>
              <Button
                size="sm"
                variant="ghost"
                onClick={() => void handleUnirVisibleEligible()}
                disabled={saving || filteredRows.filter((row) => Boolean(row.uniao_automatica_elegivel)).length === 0}
              >
                Unir pares elegiveis visiveis
              </Button>
              <Button size="sm" variant="ghost" onClick={handleExportVisiblePairs}>
                Exportar pares visiveis
              </Button>
              <Button size="sm" variant="ghost" onClick={handleExportCurrentFilter}>
                Exportar pares filtrados
              </Button>
            </div>
            <Input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Filtrar por descricao, NCM, CEST, GTIN ou recomendacao"
              className="mt-3"
            />
          </div>

          {loading ? (
            <div className="flex flex-col items-center justify-center gap-3 py-20">
              <Loader2 className="h-10 w-10 animate-spin text-slate-500" />
              <p className="text-sm text-muted-foreground">Carregando pares candidatos...</p>
            </div>
          ) : (
            <div className="overflow-auto">
              <table className="w-full min-w-[1080px] border-collapse text-sm">
                <thead className="sticky top-0 z-10 bg-slate-50">
                  <tr className="border-b">
                    <th className="w-12 px-4 py-3 text-left font-medium text-slate-700">Sel.</th>
                    <th className="px-4 py-3 text-left font-medium text-slate-700">Grupo A</th>
                    <th className="px-4 py-3 text-left font-medium text-slate-700">Grupo B</th>
                    <th className="px-4 py-3 text-left font-medium text-slate-700">
                      <button type="button" className="inline-flex items-center gap-1 font-medium text-slate-700 hover:text-slate-900" onClick={() => setSortKey("SIMILARIDADE")}>
                        Similaridade
                        {sortKey === "SIMILARIDADE" ? <span className="text-[10px] text-blue-600">ativa</span> : null}
                      </button>
                    </th>
                    <th className="px-4 py-3 text-left font-medium text-slate-700">Fiscal</th>
                    <th className="px-4 py-3 text-left font-medium text-slate-700">
                      <button type="button" className="inline-flex items-center gap-1 font-medium text-slate-700 hover:text-slate-900" onClick={() => setSortKey("RECOMENDACAO")}>
                        Recomendacao
                        {sortKey === "RECOMENDACAO" ? <span className="text-[10px] text-blue-600">ativa</span> : null}
                      </button>
                    </th>
                    <th className="px-4 py-3 text-left font-medium text-slate-700">
                      <button type="button" className="inline-flex items-center gap-1 font-medium text-slate-700 hover:text-slate-900" onClick={() => setSortKey("PRIORIDADE")}>
                        Acoes
                        {sortKey === "PRIORIDADE" ? <span className="text-[10px] text-blue-600">ativa</span> : null}
                      </button>
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRows.map((row, index) => {
                    const pairKey = getPairKey(row);
                    const checked = selectedPairKeys.includes(pairKey);
                    const signals = getPairSignals(row);
                    return (
                      <tr key={index} className={`border-b align-top ${getPairRowClass(row, checked)}`}>
                        <td className="px-4 py-4">
                          <Checkbox checked={checked} onCheckedChange={(value) => toggleSelection(row, Boolean(value))} />
                        </td>
                        <td className="px-4 py-4">
                          <div className="font-mono text-xs font-semibold text-slate-700">{normalizeValue(row.chave_produto_a)}</div>
                          <div className="mt-1 font-medium text-slate-900">{normalizeValue(row.descricao_a)}</div>
                          {signals.length > 0 ? (
                            <div className="mt-2 flex flex-wrap gap-1">
                              {signals.map((signal) => (
                                <span
                                  key={`a-${signal}`}
                                  className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${
                                    signal === "GTIN conflitante"
                                      ? "bg-rose-100 text-rose-700"
                                      : signal === "GTIN igual"
                                        ? "bg-emerald-100 text-emerald-700"
                                        : "bg-slate-100 text-slate-700"
                                  }`}
                                >
                                  {signal}
                                </span>
                              ))}
                            </div>
                          ) : null}
                          <div className="mt-1 text-xs text-slate-500">NCM {normalizeValue(row.ncm_a) || "-"} | CEST {normalizeValue(row.cest_a) || "-"} | GTIN {normalizeValue(row.gtin_a) || "-"}</div>
                        </td>
                        <td className="px-4 py-4">
                          <div className="font-mono text-xs font-semibold text-slate-700">{normalizeValue(row.chave_produto_b)}</div>
                          <div className="mt-1 font-medium text-slate-900">{normalizeValue(row.descricao_b)}</div>
                          {signals.length > 0 ? (
                            <div className="mt-2 flex flex-wrap gap-1">
                              {signals.map((signal) => (
                                <span
                                  key={`b-${signal}`}
                                  className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${
                                    signal === "GTIN conflitante"
                                      ? "bg-rose-100 text-rose-700"
                                      : signal === "GTIN igual"
                                        ? "bg-emerald-100 text-emerald-700"
                                        : "bg-slate-100 text-slate-700"
                                  }`}
                                >
                                  {signal}
                                </span>
                              ))}
                            </div>
                          ) : null}
                          <div className="mt-1 text-xs text-slate-500">NCM {normalizeValue(row.ncm_b) || "-"} | CEST {normalizeValue(row.cest_b) || "-"} | GTIN {normalizeValue(row.gtin_b) || "-"}</div>
                        </td>
                        <td className="px-4 py-4 text-xs text-slate-600">
                          <div>Final: <strong className="text-slate-800">{Math.round(Number(row.score_final || 0) * 100)}%</strong></div>
                          <div>Descricao: {Math.round(Number(row.score_descricao || 0) * 100)}%</div>
                          {similarityMode === "semantic" || row.score_semantico != null ? (
                            <div>Semantico: {Math.round(Number(row.score_semantico || 0) * 100)}%</div>
                          ) : null}
                          {row.origem_par_hibrido ? <div>Origem: {normalizeValue(row.origem_par_hibrido).replaceAll("_", " ")}</div> : null}
                        </td>
                        <td className="px-4 py-4 text-xs text-slate-600">
                          <div>NCM {Math.round(Number(row.score_ncm || 0) * 100)}%</div>
                          <div>CEST {Math.round(Number(row.score_cest || 0) * 100)}%</div>
                          <div>GTIN {Math.round(Number(row.score_gtin || 0) * 100)}%</div>
                        </td>
                        <td className="px-4 py-4 text-xs font-semibold text-slate-700">
                          <div
                            className={
                              Boolean(row.bloquear_uniao)
                                ? "text-rose-700"
                                : Boolean(row.uniao_automatica_elegivel)
                                  ? "text-emerald-700"
                                  : "text-slate-700"
                            }
                          >
                            {normalizeValue(row.recomendacao).replaceAll("_", " ")}
                          </div>
                          <div className="mt-1 text-[11px] font-normal text-slate-500">{normalizeValue(row.motivo_recomendacao)}</div>
                        </td>
                        <td className="px-4 py-4">
                          <DropdownMenu>
                            <DropdownMenuTrigger asChild>
                              <Button size="sm" variant="outline" className="h-8" disabled={saving}>
                                Acoes do par
                              </Button>
                            </DropdownMenuTrigger>
                            <DropdownMenuContent align="end" className="w-56">
                              <DropdownMenuItem onClick={() => void handlePairAction(row, "unir", 0)}>
                                Unir mantendo Grupo A
                              </DropdownMenuItem>
                              <DropdownMenuItem onClick={() => void handlePairAction(row, "unir", 1)}>
                                Unir mantendo Grupo B
                              </DropdownMenuItem>
                              <DropdownMenuItem onClick={() => void handlePairAction(row, "bloquear")}>
                                Bloquear este par
                              </DropdownMenuItem>
                              <DropdownMenuItem onClick={() => void handlePairAction(row, "verificar")}>
                                Verificado
                              </DropdownMenuItem>
                            </DropdownMenuContent>
                          </DropdownMenu>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>

        <div className="rounded-xl border border-slate-200 bg-white">
          <div className="border-b bg-slate-50 px-4 py-3">
            <div className="text-sm font-black text-slate-900">Painel de decisao</div>
            <div className="mt-1 text-xs text-slate-500">Selecione um ou mais pares. A selecao unifica os grupos envolvidos na area lateral.</div>
          </div>
          <div className="space-y-4 p-4">
            <div className="text-sm text-slate-500">Selecionados: <strong className="text-slate-700">{selectedRows.length}</strong></div>

            <div className="max-h-[38vh] space-y-2 overflow-auto pr-1">
              {selectedRows.length === 0 ? (
                <p className="text-sm text-slate-500">Nenhum grupo selecionado.</p>
              ) : (
                selectedRows.map((row) => {
                  const key = getRowKey(row);
                  const isCanonical = canonicalKey === key;
                  return (
                    <div key={key} className={`rounded-lg border px-3 py-3 ${isCanonical ? "border-blue-400 bg-blue-50" : "border-slate-200"}`}>
                      <div className="flex items-start justify-between gap-3">
                        <div className="space-y-1">
                          <div className="font-mono text-xs text-slate-500">{normalizeValue(row.chave_produto)}</div>
                          <div className="text-sm font-medium text-slate-900">{getRowDescription(row)}</div>
                        </div>
                        <Button size="sm" variant={isCanonical ? "default" : "outline"} className="h-8" onClick={() => setCanonicalKey(key)}>
                          {isCanonical ? "Canonica" : "Definir"}
                        </Button>
                      </div>
                      <div className="mt-2 text-xs text-slate-500">
                        NCM {normalizeValue(row.ncm_consenso) || "-"} | CEST {normalizeValue(row.cest_consenso) || "-"} | GTIN {normalizeValue(row.gtin_consenso) || "-"}
                      </div>
                    </div>
                  );
                })
              )}
            </div>

            <Separator />

            <div className="grid gap-2">
              <Button variant="outline" className="h-10 gap-2" disabled={saving || selectedRows.length < 1} onClick={() => void handleMarkVerified()}>
                {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <CheckCircle2 className="h-4 w-4" />}
                Marcar como verificado
              </Button>
              <Button variant="outline" className="h-10 gap-2" disabled={saving || selectedRows.length < 1} onClick={() => void handleUndoVerified()}>
                {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <ArrowLeftRight className="h-4 w-4" />}
                Desfazer verificado
              </Button>
              <Button className="h-10 gap-2" disabled={saving || selectedRows.length < 2 || !canonicalKey} onClick={() => void submitRules("unir")}>
                {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <GitMerge className="h-4 w-4" />}
                Unir grupos selecionados
              </Button>
              <Button variant="outline" className="h-10 gap-2" disabled={saving || selectedRows.length < 2} onClick={() => void submitRules("separar")}>
                {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <SplitSquareHorizontal className="h-4 w-4" />}
                Manter grupos separados
              </Button>
              <Button variant="ghost" className="h-10 gap-2" disabled={saving || selectedRows.length < 2} onClick={() => void handleUndoRules()}>
                {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <ArrowLeftRight className="h-4 w-4" />}
                Desfazer regras entre selecionados
              </Button>
            </div>

            <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-500">
              <strong className="text-slate-700">Unir</strong> grava regras <code>UNIR_GRUPOS</code> e reprocessa o pipeline.
              <br />
              <strong className="text-slate-700">Manter separados</strong> grava regras <code>MANTER_SEPARADO</code> no mapa manual de descricoes.
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
