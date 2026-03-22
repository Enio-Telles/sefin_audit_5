import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Loader2, Plus, ChevronRight, X } from "lucide-react";
import {
  desfazerDecisaoCodigo,
  getCodigoMultiDescricaoResumo,
  getProdutoDetalhes,
  resolverManualDesagregar,
  getNcmDetails,
  getCestDetails,
  type CodigoMultiDescricaoGrupoResumo,
  type NcmDetailsResponse,
  type CestDetailsResponse,
} from "@/lib/pythonApi";
import { similarityScore } from "@/lib/productSimilarity";
import { toast } from "sonner";
import { Label } from "@/components/ui/label";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";

interface DesagregarProdutosContentProps {
  cnpj: string;
  codigo: string;
  onSuccess: () => void;
  onCancel: () => void;
  embedded?: boolean;
}

interface ProdutoDetalhe {
  fonte: string;
  codigo: string;
  descricao: string;
  ncm: string;
  cest: string;
  gtin: string;
  tipo_item: string;
  descr_compl_c170: string;
  codigo_original?: string;
  descricao_original?: string;
  co_emitente?: string;
}

interface GrupoDesagregacao {
  id: string;
  codigo_novo: string;
  descricao_nova: string;
  ncm_novo: string;
  cest_novo: string;
  gtin_novo: string;
  grupos_origem: CodigoMultiDescricaoGrupoResumo[];
}

function normalizeText(value: unknown): string {
  return String(value ?? "").trim();
}

function canonicalText(value: unknown): string {
  return normalizeText(value).toUpperCase();
}

function analyzeGroupPairs(gruposDescricao: CodigoMultiDescricaoGrupoResumo[]) {
  let mostSimilar: { a: string; b: string; score: number } | null = null;
  let mostDissimilar: { a: string; b: string; score: number } | null = null;

  for (let i = 0; i < gruposDescricao.length; i += 1) {
    for (let j = i + 1; j < gruposDescricao.length; j += 1) {
      const a = gruposDescricao[i].descricao;
      const b = gruposDescricao[j].descricao;
      const score = similarityScore(a, b);

      if (!mostSimilar || score > mostSimilar.score) {
        mostSimilar = { a, b, score };
      }
      if (!mostDissimilar || score < mostDissimilar.score) {
        mostDissimilar = { a, b, score };
      }
    }
  }

  return { mostSimilar, mostDissimilar };
}

function getPrimaryValue(value: string): string {
  return normalizeText(value.split(",")[0]);
}

function fiscalAffinity(a: CodigoMultiDescricaoGrupoResumo, b: CodigoMultiDescricaoGrupoResumo): number {
  let matches = 0;
  const aNcm = getPrimaryValue(a.lista_ncm);
  const bNcm = getPrimaryValue(b.lista_ncm);
  const aCest = getPrimaryValue(a.lista_cest);
  const bCest = getPrimaryValue(b.lista_cest);
  const aGtin = getPrimaryValue(a.lista_gtin);
  const bGtin = getPrimaryValue(b.lista_gtin);
  if (aNcm && bNcm && aNcm === bNcm) matches += 1;
  if (aCest && bCest && aCest === bCest) matches += 1;
  if (aGtin && bGtin && aGtin === bGtin) matches += 1;
  return matches;
}

function buildSuggestedClusters(gruposDescricao: CodigoMultiDescricaoGrupoResumo[]): CodigoMultiDescricaoGrupoResumo[][] {
  const ordered = [...gruposDescricao]
    .sort((a, b) => {
      if (b.qtd_linhas !== a.qtd_linhas) return b.qtd_linhas - a.qtd_linhas;
      return a.descricao.localeCompare(b.descricao);
    });

  const visited = new Set<string>();
  const clusters: CodigoMultiDescricaoGrupoResumo[][] = [];

  ordered.forEach((seed) => {
    if (visited.has(seed.descricao)) return;
    const cluster: CodigoMultiDescricaoGrupoResumo[] = [];
    const queue = [seed];
    visited.add(seed.descricao);

    while (queue.length > 0) {
      const current = queue.shift()!;
      cluster.push(current);
      ordered.forEach((candidate) => {
        if (visited.has(candidate.descricao)) return;
        const score = similarityScore(current.descricao, candidate.descricao);
        const fiscalScore = fiscalAffinity(current, candidate);
        const shouldJoin = score >= 0.46 || (score >= 0.34 && fiscalScore >= 2);
        if (shouldJoin) {
          visited.add(candidate.descricao);
          queue.push(candidate);
        }
      });
    }

    clusters.push(cluster);
  });

  return clusters;
}

function buildInitialGroups(codigo: string, gruposDescricao: CodigoMultiDescricaoGrupoResumo[]): GrupoDesagregacao[] {
  return buildSuggestedClusters(gruposDescricao).map((cluster, index) => ({
      id: crypto.randomUUID(),
      codigo_novo: index === 0 ? codigo : `${codigo}_${index}`,
      descricao_nova: cluster[0]?.descricao || "",
      ncm_novo: getPrimaryValue(cluster[0]?.lista_ncm || ""),
      cest_novo: getPrimaryValue(cluster[0]?.lista_cest || ""),
      gtin_novo: getPrimaryValue(cluster[0]?.lista_gtin || ""),
      grupos_origem: cluster,
    }));
}

function compactGroups(grupos: GrupoDesagregacao[], codigoBase: string): GrupoDesagregacao[] {
  return grupos
    .filter((grupo) => grupo.grupos_origem.length > 0)
    .map((grupo, index) => ({
      ...grupo,
      codigo_novo: index === 0 ? codigoBase : `${codigoBase}_${index}`,
    }));
}

const DRAFT_MAX_AGE_DAYS = 7;

function isStaleDraft(savedAt?: string): boolean {
  if (!savedAt) return false;
  const savedTime = new Date(savedAt).getTime();
  if (!Number.isFinite(savedTime)) return false;
  const maxAgeMs = DRAFT_MAX_AGE_DAYS * 24 * 60 * 60 * 1000;
  return Date.now() - savedTime > maxAgeMs;
}

const GROUP_ACCENTS = [
  "border-l-blue-500",
  "border-l-emerald-500",
  "border-l-amber-500",
  "border-l-rose-500",
  "border-l-cyan-500",
];

export function DesagregarProdutosContent({
  cnpj,
  codigo,
  onSuccess,
  onCancel,
  embedded = false,
}: DesagregarProdutosContentProps) {
  const storageKey = `produto-popup-separar:${cnpj}:${codigo}`;
  const [draftRestored, setDraftRestored] = useState(false);
  const [draftSavedAt, setDraftSavedAt] = useState<string>("");
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [undoing, setUndoing] = useState(false);
  const [resumo, setResumo] = useState<Record<string, unknown>>({});
  const [gruposDisponiveis, setGruposDisponiveis] = useState<CodigoMultiDescricaoGrupoResumo[]>([]);
  const [grupos, setGrupos] = useState<GrupoDesagregacao[]>([]);
  const [selectedDescriptions, setSelectedDescriptions] = useState<string[]>([]);
  const [ncmDetailsMap, setNcmDetailsMap] = useState<Record<string, NcmDetailsResponse["data"]>>({});
  const [cestDetailsMap, setCestDetailsMap] = useState<Record<string, CestDetailsResponse["data"]>>({});
  const [loadingFiscais, setLoadingFiscais] = useState<Record<string, boolean>>({});
  const pairAnalysis = analyzeGroupPairs(gruposDisponiveis);
  const suggestedClusters = buildSuggestedClusters(gruposDisponiveis);
  const suggestedClusterMap = new Map(
    suggestedClusters.flatMap((cluster, clusterIndex) =>
      cluster.map((item) => [item.descricao, clusterIndex] as const)
    )
  );

  const totalLinhas =
    Number(normalizeText(resumo.qtd_linhas)) ||
    gruposDisponiveis.reduce((acc, grupo) => acc + grupo.qtd_linhas, 0);

  useEffect(() => {
    if (!cnpj || !codigo || gruposDisponiveis.length === 0) return;
    try {
      const raw = window.sessionStorage.getItem(storageKey);
      if (!raw) return;
      const state = JSON.parse(raw) as {
        selectedDescriptions?: string[];
        savedAt?: string;
        grupos?: Array<{
          id: string;
          codigo_novo: string;
          descricao_nova: string;
          ncm_novo: string;
          cest_novo: string;
          gtin_novo: string;
          grupos_origem: string[];
        }>;
      };
      if (isStaleDraft(state.savedAt)) {
        const shouldRestore = window.confirm(
          `Existe um rascunho salvo ha mais de ${DRAFT_MAX_AGE_DAYS} dias para este codigo. Deseja restaurar mesmo assim?`
        );
        if (!shouldRestore) {
          window.sessionStorage.removeItem(storageKey);
          return;
        }
      }
      if (Array.isArray(state.selectedDescriptions)) {
        setDraftRestored(true);
        setDraftSavedAt(state.savedAt || "");
        setSelectedDescriptions(state.selectedDescriptions);
      }
      if (Array.isArray(state.grupos) && state.grupos.length > 0) {
        const origemMap = new Map(gruposDisponiveis.map((grupo) => [grupo.descricao, grupo] as const));
        const restored = state.grupos
          .map((grupo) => ({
            ...grupo,
            grupos_origem: (grupo.grupos_origem || [])
              .map((descricao) => origemMap.get(descricao))
              .filter((item): item is CodigoMultiDescricaoGrupoResumo => Boolean(item)),
          }))
          .filter((grupo) => grupo.grupos_origem.length > 0);
        if (restored.length > 0) {
          setDraftRestored(true);
          setDraftSavedAt(state.savedAt || "");
          setGrupos(compactGroups(restored, codigo));
        }
      }
    } catch {
      // ignore invalid popup state
    }
  }, [cnpj, codigo, storageKey, gruposDisponiveis]);

  useEffect(() => {
    if (!cnpj || !codigo || grupos.length === 0) return;
    const savedAt = new Date().toISOString();
    setDraftSavedAt(savedAt);
    window.sessionStorage.setItem(
      storageKey,
      JSON.stringify({
        savedAt,
        selectedDescriptions,
        grupos: grupos.map((grupo) => ({
          id: grupo.id,
          codigo_novo: grupo.codigo_novo,
          descricao_nova: grupo.descricao_nova,
          ncm_novo: grupo.ncm_novo,
          cest_novo: grupo.cest_novo,
          gtin_novo: grupo.gtin_novo,
          grupos_origem: grupo.grupos_origem.map((origem) => origem.descricao),
        })),
      })
    );
  }, [cnpj, codigo, storageKey, grupos, selectedDescriptions]);

  useEffect(() => {
    const baseTitle = `Separar codigo ${codigo}`;
    document.title = draftRestored ? `${baseTitle} [rascunho]` : baseTitle;
    return () => {
      document.title = "SEFIN Audit Tool";
    };
  }, [codigo, draftRestored]);

  useEffect(() => {
    if (codigo) {
      void loadResumo();
    }
  }, [codigo, cnpj]);

  useEffect(() => {
    grupos.forEach((grupo) => {
      if (grupo.ncm_novo && !ncmDetailsMap[grupo.ncm_novo] && !loadingFiscais[`ncm_${grupo.ncm_novo}`]) {
        setLoadingFiscais((prev) => ({ ...prev, [`ncm_${grupo.ncm_novo}`]: true }));
        getNcmDetails(grupo.ncm_novo)
          .then((res) => {
            if (res.success) setNcmDetailsMap((prev) => ({ ...prev, [grupo.ncm_novo]: res.data }));
          })
          .finally(() => setLoadingFiscais((prev) => ({ ...prev, [`ncm_${grupo.ncm_novo}`]: false })));
      }
      if (grupo.cest_novo && !cestDetailsMap[grupo.cest_novo] && !loadingFiscais[`cest_${grupo.cest_novo}`]) {
        setLoadingFiscais((prev) => ({ ...prev, [`cest_${grupo.cest_novo}`]: true }));
        getCestDetails(grupo.cest_novo)
          .then((res) => {
            if (res.success) setCestDetailsMap((prev) => ({ ...prev, [grupo.cest_novo]: res.data }));
          })
          .finally(() => setLoadingFiscais((prev) => ({ ...prev, [`cest_${grupo.cest_novo}`]: false })));
      }
    });
  }, [grupos, ncmDetailsMap, cestDetailsMap, loadingFiscais]);

  const loadResumo = async () => {
    setLoading(true);
    try {
      const res = await getCodigoMultiDescricaoResumo(cnpj, codigo);
      if (!res.success) {
        setResumo({});
        setGruposDisponiveis([]);
        setGrupos([]);
        return;
      }
      const gruposResumo = res.grupos_descricao || [];
      setResumo(res.resumo || {});
      setGruposDisponiveis(gruposResumo);
      setGrupos(buildInitialGroups(codigo, gruposResumo));
    } catch {
      toast.error("Erro ao carregar resumo para separacao.");
    } finally {
      setLoading(false);
    }
  };

  const handleConfirm = async () => {
    if (grupos.some((grupo) => grupo.grupos_origem.length === 0)) {
      toast.error("Existem grupos vazios. Mova descricoes ou remova o grupo.");
      return;
    }
    if (grupos.some((grupo) => !grupo.descricao_nova.trim())) {
      toast.error("Todos os grupos precisam ter uma descricao valida.");
      return;
    }

    setSaving(true);
    try {
      const detalhes = await getProdutoDetalhes(cnpj, codigo) as any;
      if (!detalhes.success) {
        toast.error("Nao foi possivel carregar os detalhes brutos do codigo.");
        return;
      }

      const destinoPorDescricao = new Map<string, GrupoDesagregacao>();
      grupos.forEach((grupo) => {
        grupo.grupos_origem.forEach((origem) => {
          destinoPorDescricao.set(canonicalText(origem.descricao), grupo);
        });
      });

      const itensDecididos = detalhes.itens.map((item: ProdutoDetalhe) => {
        const chaveDescricao = canonicalText(item.descricao || item.descricao_original);
        const grupoDestino = destinoPorDescricao.get(chaveDescricao);
        if (!grupoDestino) {
          throw new Error(`Descricao sem grupo de destino: ${item.descricao || item.descricao_original}`);
        }
        return {
          fonte: item.fonte,
          codigo_original: item.codigo_original || item.codigo,
          descricao_original: item.descricao_original || item.descricao,
          descricao_ori: item.descricao_original || item.descricao,
          tipo_item_original: item.tipo_item || "",
          codigo_novo: grupoDestino.codigo_novo,
          descricao_nova: grupoDestino.descricao_nova,
          ncm_novo: grupoDestino.ncm_novo,
          cest_novo: grupoDestino.cest_novo,
          gtin_novo: grupoDestino.gtin_novo,
          tipo_item_novo: item.tipo_item || "",
          tipo_item: item.tipo_item || "",
          co_emitente: item.co_emitente,
        };
      });

      const res = await resolverManualDesagregar(cnpj, itensDecididos);
      if (res.status === "sucesso") {
        window.sessionStorage.removeItem(storageKey);
        setDraftRestored(false);
        setDraftSavedAt("");
        toast.success(res.mensagem);
        onSuccess();
      } else {
        toast.error(res.mensagem);
      }
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Erro ao salvar separacao.");
    } finally {
      setSaving(false);
    }
  };

  const handleUndo = async () => {
    setUndoing(true);
    try {
      const res = await desfazerDecisaoCodigo(cnpj, codigo);
      window.sessionStorage.removeItem(storageKey);
      setDraftRestored(false);
      setDraftSavedAt("");
      toast.success(res.mensagem);
      onSuccess();
    } catch {
      toast.error("Erro ao desfazer separacao.");
    } finally {
      setUndoing(false);
    }
  };

  const handleCriarNovoGrupo = () => {
    const novoIndex = grupos.length;
    setGrupos([
      ...grupos,
      {
        id: crypto.randomUUID(),
        codigo_novo: `${codigo}_${novoIndex}`,
        descricao_nova: "",
        ncm_novo: "",
        cest_novo: "",
        gtin_novo: "",
        grupos_origem: [],
      },
    ]);
  };

  const handleMoverDescricoesParaGrupo = (grupoDestinoId: string) => {
    if (selectedDescriptions.length === 0) return;
    const descricoesSelecionadas = gruposDisponiveis.filter((grupo) => selectedDescriptions.includes(grupo.descricao));

    setGrupos((prev) => {
      const novosGrupos = prev.map((grupo) => ({
        ...grupo,
        grupos_origem: grupo.grupos_origem.filter((origem) => !selectedDescriptions.includes(origem.descricao)),
      }));
      const destino = novosGrupos.find((grupo) => grupo.id === grupoDestinoId);
      if (destino) {
        destino.grupos_origem.push(...descricoesSelecionadas);
        if (!destino.descricao_nova && descricoesSelecionadas.length > 0) {
          destino.descricao_nova = descricoesSelecionadas[0].descricao;
          if (!destino.ncm_novo) destino.ncm_novo = normalizeText(descricoesSelecionadas[0].lista_ncm.split(",")[0]);
          if (!destino.cest_novo) destino.cest_novo = normalizeText(descricoesSelecionadas[0].lista_cest.split(",")[0]);
          if (!destino.gtin_novo) destino.gtin_novo = normalizeText(descricoesSelecionadas[0].lista_gtin.split(",")[0]);
        }
      }
      return compactGroups(novosGrupos, codigo);
    });

    setSelectedDescriptions([]);
  };

  const toggleDescricaoSelection = (descricao: string) => {
    setSelectedDescriptions((prev) =>
      prev.includes(descricao) ? prev.filter((item) => item !== descricao) : [...prev, descricao]
    );
  };

  const atualizarGrupo = (id: string, campo: keyof GrupoDesagregacao, valor: string) => {
    setGrupos((prev) => prev.map((grupo) => (grupo.id === id ? { ...grupo, [campo]: valor } : grupo)));
  };

  const removerGrupo = (id: string) => {
    setGrupos((prev) => {
      const removido = prev.find((grupo) => grupo.id === id);
      if (!removido) return prev;
      const novos = prev.filter((grupo) => grupo.id !== id);
      if (novos.length > 0) {
        novos[0].grupos_origem.push(...removido.grupos_origem);
      }
      return compactGroups(novos, codigo);
    });
  };

  const clearDraft = () => {
    window.sessionStorage.removeItem(storageKey);
    setDraftRestored(false);
    setDraftSavedAt("");
    setSelectedDescriptions([]);
    setGrupos(buildInitialGroups(codigo, gruposDisponiveis));
  };

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3 py-20">
        <Loader2 className="h-10 w-10 animate-spin text-purple-600" />
        <p className="text-sm font-black text-slate-600 uppercase tracking-widest">Sincronizando resumo do codigo...</p>
      </div>
    );
  }

  return (
    <div className={`flex flex-col h-full bg-slate-50 ${embedded ? "" : "p-0"}`}>
      <div className="flex-1 overflow-hidden">
        <div className="h-full grid grid-cols-1 lg:grid-cols-[0.95fr_1.05fr] divide-x divide-slate-200 overflow-hidden">
          <div className="flex h-full flex-col overflow-hidden bg-white">
            <div className="border-b bg-white px-4 py-3 shrink-0">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <h3 className="text-[11px] font-black uppercase tracking-widest text-slate-700">Descricoes do codigo</h3>
                  <div className="mt-1 text-xs text-slate-500">
                    {gruposDisponiveis.length} descricoes | {totalLinhas} linhas | {selectedDescriptions.length} selecionadas
                  </div>
                  <div className="mt-2 text-xs text-slate-500">
                    Selecione uma ou mais descricoes e use <span className="font-semibold text-slate-700">Mover</span> no grupo de destino para manter itens juntos.
                  </div>
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={handleUndo}
                  disabled={loading || saving || undoing}
                  className="shrink-0"
                >
                  {undoing ? <Loader2 className="h-4 w-4 animate-spin" /> : "Desfazer decisao"}
                </Button>
              </div>
              {draftRestored ? (
                <div className="mt-3 flex items-center justify-between gap-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
                  <span>
                    Rascunho restaurado automaticamente para este codigo.
                    {draftSavedAt ? ` Ultimo salvamento: ${new Date(draftSavedAt).toLocaleString("pt-BR")}.` : ""}
                  </span>
                  <Button variant="ghost" size="sm" className="h-7 px-2 text-amber-900" onClick={clearDraft}>
                    Limpar rascunho
                  </Button>
                </div>
              ) : null}
              {pairAnalysis.mostSimilar || pairAnalysis.mostDissimilar ? (
                <div className="mt-3 grid gap-2 md:grid-cols-2">
                  {pairAnalysis.mostSimilar ? (
                    <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
                      <div className="text-[10px] font-black uppercase tracking-widest text-slate-500">Par mais similar</div>
                      <div className="mt-1 font-medium text-slate-800">{pairAnalysis.mostSimilar.a}</div>
                      <div className="text-slate-500">{pairAnalysis.mostSimilar.b}</div>
                      <div className="mt-1">Similaridade: {(pairAnalysis.mostSimilar.score * 100).toFixed(0)}%</div>
                    </div>
                  ) : null}
                  {pairAnalysis.mostDissimilar ? (
                    <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
                      <div className="text-[10px] font-black uppercase tracking-widest text-slate-500">Par mais dissimilar</div>
                      <div className="mt-1 font-medium text-slate-800">{pairAnalysis.mostDissimilar.a}</div>
                      <div className="text-slate-500">{pairAnalysis.mostDissimilar.b}</div>
                      <div className="mt-1">Similaridade: {(pairAnalysis.mostDissimilar.score * 100).toFixed(0)}%</div>
                    </div>
                  ) : null}
                </div>
              ) : null}
            </div>
            <ScrollArea className="flex-1 p-4">
              <div className="space-y-2 pb-8">
                {gruposDisponiveis.map((grupoResumo) => {
                  const isSelected = selectedDescriptions.includes(grupoResumo.descricao);
                  const grupoDestinoIdx = grupos.findIndex((grupo) =>
                    grupo.grupos_origem.some((origem) => origem.descricao === grupoResumo.descricao)
                  );
                  const suggestedClusterIdx = suggestedClusterMap.get(grupoResumo.descricao) ?? 0;
                  const suggestedAccent = GROUP_ACCENTS[suggestedClusterIdx % GROUP_ACCENTS.length];

                  return (
                    <button
                      key={grupoResumo.descricao}
                      type="button"
                      onClick={() => toggleDescricaoSelection(grupoResumo.descricao)}
                      className={`w-full rounded-lg border-l-4 ${suggestedAccent} px-3 py-3 text-left transition ${
                        isSelected
                          ? "border-purple-500 bg-purple-50"
                          : "border-slate-200 bg-white hover:border-slate-300"
                      }`}
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0 flex-1">
                          <div className="text-sm font-semibold uppercase leading-snug text-slate-900">
                            {grupoResumo.descricao}
                          </div>
                          <div className="mt-1 text-xs text-slate-500">
                            {grupoResumo.qtd_linhas} linha{grupoResumo.qtd_linhas > 1 ? "s" : ""}
                            {grupoDestinoIdx >= 0 ? ` | grupo ${grupoDestinoIdx + 1}` : ""}
                            {` | sugestao ${suggestedClusterIdx + 1}`}
                          </div>
                          <div className="mt-1 text-xs text-slate-500">
                            Compl. {grupoResumo.lista_descr_compl || "-"} | NCM {grupoResumo.lista_ncm || "-"}
                          </div>
                          <div className="text-xs text-slate-500">
                            GTIN {grupoResumo.lista_gtin || "-"} | Tipo {grupoResumo.lista_tipo_item || "-"}
                          </div>
                        </div>
                        <div
                          className={`mt-1 h-4 w-4 rounded-full border ${
                            isSelected ? "border-purple-600 bg-purple-600" : "border-slate-300 bg-white"
                          }`}
                        >
                          {isSelected ? <div className="m-auto mt-[3px] h-2 w-2 rounded-full bg-white" /> : null}
                        </div>
                      </div>
                    </button>
                  );
                })}
              </div>
            </ScrollArea>
          </div>

          <div className="flex h-full flex-col overflow-hidden bg-slate-50">
            <div className="border-b bg-white px-4 py-3 shrink-0 flex items-center justify-between gap-3">
              <div>
                <h3 className="text-[11px] font-black uppercase tracking-widest text-slate-700">Novos produtos</h3>
                <div className="mt-1 text-xs text-slate-500">
                  {grupos.length} grupos | {grupos.reduce((acc, grupo) => acc + grupo.grupos_origem.length, 0)} descricoes alocadas
                </div>
              </div>
              <Button
                size="sm"
                onClick={handleCriarNovoGrupo}
                className="h-8 px-3 bg-purple-600 hover:bg-purple-700 font-black text-[10px] uppercase tracking-widest rounded-md"
              >
                <Plus className="h-3.5 w-3.5 mr-1" />
                Novo grupo
              </Button>
            </div>
            <ScrollArea className="flex-1 p-4">
              <div className="space-y-4 pb-20">
                {grupos.map((grupo, index) => {
                  const accent = GROUP_ACCENTS[index % GROUP_ACCENTS.length];

                  return (
                    <div
                      key={grupo.id}
                      className={`rounded-xl border border-slate-200 bg-white p-4 ${grupo.grupos_origem.length === 0 ? "opacity-60" : ""}`}
                    >
                      <div className={`border-l-4 ${accent} pl-3`}>
                        <div className="flex items-start justify-between gap-3">
                          <div>
                            <div className="text-[10px] font-black uppercase tracking-widest text-slate-500">Grupo {index + 1}</div>
                            <div className="mt-1 text-xs text-slate-500">
                              {grupo.grupos_origem.length} descricao{grupo.grupos_origem.length > 1 ? "es" : ""} alocada{grupo.grupos_origem.length > 1 ? "s" : ""}
                            </div>
                          </div>
                          <div className="flex gap-2">
                            {selectedDescriptions.length > 0 ? (
                              <Button
                                size="sm"
                                variant="outline"
                                className="h-8 px-3 text-[10px] font-black uppercase tracking-widest"
                                onClick={() => handleMoverDescricoesParaGrupo(grupo.id)}
                              >
                                <ChevronRight className="h-3.5 w-3.5 mr-1" />
                                Mover
                              </Button>
                            ) : null}
                            {index > 0 ? (
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-8 w-8 text-slate-400 hover:text-red-500"
                                onClick={() => removerGrupo(grupo.id)}
                              >
                                <X className="h-4 w-4" />
                              </Button>
                            ) : null}
                          </div>
                        </div>
                      </div>

                      <div className="mt-4 grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
                        <div className="space-y-1.5">
                          <Label className="text-[10px] font-black text-slate-500 uppercase tracking-widest">Codigo</Label>
                          <Input value={grupo.codigo_novo} onChange={(e) => atualizarGrupo(grupo.id, "codigo_novo", e.target.value)} className="h-10 font-mono font-black border-slate-200 bg-slate-50" />
                        </div>
                        <div className="space-y-1.5 md:col-span-2">
                          <Label className="text-[10px] font-black text-slate-500 uppercase tracking-widest">Descricao</Label>
                          <Input value={grupo.descricao_nova} onChange={(e) => atualizarGrupo(grupo.id, "descricao_nova", e.target.value)} className="h-10 border-slate-200 bg-slate-50 font-semibold" placeholder="Descricao oficial do grupo" />
                        </div>
                        <div className="space-y-1.5">
                          <Label className="text-[10px] font-black text-slate-500 uppercase tracking-widest">NCM</Label>
                          <Input value={grupo.ncm_novo} onChange={(e) => atualizarGrupo(grupo.id, "ncm_novo", e.target.value)} className="h-10 font-mono border-slate-200 bg-slate-50" />
                        </div>
                        <div className="space-y-1.5">
                          <Label className="text-[10px] font-black text-slate-500 uppercase tracking-widest">CEST</Label>
                          <Input value={grupo.cest_novo} onChange={(e) => atualizarGrupo(grupo.id, "cest_novo", e.target.value)} className="h-10 font-mono border-slate-200 bg-slate-50" />
                        </div>
                        <div className="space-y-1.5">
                          <Label className="text-[10px] font-black text-slate-500 uppercase tracking-widest">GTIN</Label>
                          <Input value={grupo.gtin_novo} onChange={(e) => atualizarGrupo(grupo.id, "gtin_novo", e.target.value)} className="h-10 font-mono border-slate-200 bg-slate-50" />
                        </div>
                      </div>

                      {(grupo.ncm_novo || grupo.cest_novo) && (
                        <div className="mt-4 rounded-lg border border-slate-200 bg-slate-50 p-3 space-y-3">
                          {grupo.ncm_novo && ncmDetailsMap[grupo.ncm_novo] ? (
                            <div className="text-[11px] space-y-1">
                              <p className="font-black uppercase tracking-wider text-slate-600">NCM {grupo.ncm_novo}</p>
                              <div className="text-slate-600 whitespace-pre-wrap">{ncmDetailsMap[grupo.ncm_novo].descricao}</div>
                            </div>
                          ) : null}
                          {grupo.cest_novo && cestDetailsMap[grupo.cest_novo] ? (
                            <div className="border-t border-slate-200 pt-3 text-[11px] space-y-1">
                              <p className="font-black uppercase tracking-wider text-slate-600">CEST {grupo.cest_novo}</p>
                              <div className="text-slate-600 whitespace-pre-wrap">
                                {cestDetailsMap[grupo.cest_novo].descricoes?.[0] || cestDetailsMap[grupo.cest_novo].nome_segmento}
                              </div>
                            </div>
                          ) : null}
                          {!ncmDetailsMap[grupo.ncm_novo] && !cestDetailsMap[grupo.cest_novo] ? (
                            <div className="text-[10px] font-black uppercase tracking-widest text-slate-400">
                              Buscando informacoes fiscais...
                            </div>
                          ) : null}
                        </div>
                      )}

                      <div className="mt-4 rounded-lg border border-slate-200 bg-slate-50 p-3">
                        <div className="text-[10px] font-black uppercase tracking-widest text-slate-500">Descricoes alocadas</div>
                        <div className="mt-2 space-y-1 text-xs text-slate-600">
                          {grupo.grupos_origem.length > 0 ? (
                            grupo.grupos_origem.map((origem) => (
                              <div key={origem.descricao}>
                                {origem.descricao} ({origem.qtd_linhas})
                              </div>
                            ))
                          ) : (
                            <div>Nenhuma descricao alocada.</div>
                          )}
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </ScrollArea>
          </div>
        </div>
      </div>

      <div className={`p-4 border-t bg-white flex justify-between items-center shrink-0 z-30 ${embedded ? "" : "px-8"}`}>
        <div className="flex flex-col">
          <span className="text-[10px] font-black text-slate-400 uppercase tracking-tighter">Resumo</span>
          <div className="flex items-center gap-3">
            <span className="text-[14px] font-black text-slate-700">{grupos.length} GRUPOS</span>
            <Separator orientation="vertical" className="h-3 bg-slate-300" />
            <span className="text-[14px] font-black text-slate-400">
              {grupos.reduce((acc, grupo) => acc + grupo.grupos_origem.length, 0)} / {gruposDisponiveis.length} DESCRICOES ALOCADAS
            </span>
          </div>
        </div>
        <div className="flex gap-3">
          <Button variant="ghost" onClick={onCancel} className="h-10 px-6 text-[11px] font-black uppercase tracking-widest text-slate-500 hover:text-slate-800 rounded-xl">
            Voltar
          </Button>
          <Button disabled={loading || saving || undoing || selectedDescriptions.length > 0} onClick={handleConfirm} className="h-12 px-12 bg-purple-600 hover:bg-purple-700 font-black text-[14px] uppercase tracking-widest rounded-xl">
            {saving ? (
              <>
                <Loader2 className="h-5 w-5 mr-2 animate-spin" />
                Processando...
              </>
            ) : "Concluir separacao do codigo"}
          </Button>
        </div>
      </div>
    </div>
  );
}
