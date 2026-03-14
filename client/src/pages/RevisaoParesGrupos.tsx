import { useEffect, useMemo, useState } from "react";
import { useLocation } from "wouter";
import { ArrowLeftRight, ChevronLeft, GitMerge, Loader2, RefreshCw, SplitSquareHorizontal } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import { toast } from "sonner";
import { desfazerManualDescricoes, getProdutosRevisaoManual, resolverManualDescricoes, type DescricaoManualMapItem } from "@/lib/pythonApi";

type ReviewRow = Record<string, unknown>;

function normalizeValue(value: unknown): string {
  return String(value ?? "").trim();
}

function getRowDescription(row: ReviewRow): string {
  return normalizeValue(row.descricao);
}

function getRowKey(row: ReviewRow): string {
  return normalizeValue(row.chave_produto) || getRowDescription(row);
}

function countSelectedCodes(rows: ReviewRow[]): number {
  return rows.reduce((acc, row) => acc + Number(normalizeValue(row.qtd_codigos) || 0), 0);
}

function StatBox({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-lg border border-slate-200 bg-white px-4 py-3">
      <div className="text-[10px] font-black uppercase tracking-widest text-slate-500">{label}</div>
      <div className="mt-1 text-2xl font-black text-slate-900">{value}</div>
    </div>
  );
}

export default function RevisaoParesGrupos() {
  const [, navigate] = useLocation();
  const searchParams = new URLSearchParams(window.location.search);
  const cnpj = searchParams.get("cnpj") || "";

  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [rows, setRows] = useState<ReviewRow[]>([]);
  const [selectedKeys, setSelectedKeys] = useState<string[]>([]);
  const [canonicalKey, setCanonicalKey] = useState<string>("");
  const [search, setSearch] = useState("");

  const loadRows = async () => {
    if (!cnpj) {
      setRows([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      const res = await getProdutosRevisaoManual(cnpj);
      setRows(res.success ? res.data : []);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Falha ao carregar grupos pendentes.";
      toast.error("Erro ao carregar revisao", { description: message });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadRows();
  }, [cnpj]);

  const filteredRows = useMemo(() => {
    const term = search.trim().toUpperCase();
    if (!term) return rows;

    return rows.filter((row) => {
      return [
        row.chave_produto,
        row.descricao,
        row.lista_descricao,
        row.ncm_consenso,
        row.cest_consenso,
        row.gtin_consenso,
        row.descricoes_conflitantes,
      ].some((value) => normalizeValue(value).toUpperCase().includes(term));
    });
  }, [rows, search]);

  const selectedRows = useMemo(() => {
    const selectedSet = new Set(selectedKeys);
    return rows.filter((row) => selectedSet.has(getRowKey(row)));
  }, [rows, selectedKeys]);

  useEffect(() => {
    if (!canonicalKey || !selectedKeys.includes(canonicalKey)) {
      setCanonicalKey(selectedKeys[0] || "");
    }
  }, [selectedKeys, canonicalKey]);

  const toggleSelection = (row: ReviewRow, checked: boolean) => {
    const key = getRowKey(row);
    setSelectedKeys((current) => {
      if (checked) {
        return current.includes(key) ? current : [...current, key];
      }
      return current.filter((item) => item !== key);
    });
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
      setSelectedKeys([]);
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
      setSelectedKeys([]);
      setCanonicalKey("");
      await loadRows();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Falha ao remover regras manuais de descricoes.";
      toast.error("Erro ao desfazer regras", { description: message });
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
            <p className="mt-1 text-sm text-slate-500">CNPJ {cnpj} | Uniao manual de grupos ou bloqueio de convergencia.</p>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
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

      <div className="grid gap-3 md:grid-cols-3">
        <StatBox label="Grupos pendentes" value={rows.length} />
        <StatBox label="Selecionados" value={selectedRows.length} />
        <StatBox label="Codigos envolvidos" value={countSelectedCodes(selectedRows)} />
      </div>

      <div className="grid gap-5 xl:grid-cols-[1.45fr_0.85fr]">
        <div className="overflow-hidden rounded-xl border border-slate-200 bg-white">
          <div className="border-b bg-slate-50 px-4 py-3">
            <div className="text-sm font-black text-slate-900">Grupos pendentes</div>
            <div className="mt-1 text-xs text-slate-500">A lista inclui apenas grupos com requer_revisao_manual = true.</div>
            <Input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Filtrar por descricao, NCM, CEST, GTIN ou conflito"
              className="mt-3"
            />
          </div>

          {loading ? (
            <div className="flex flex-col items-center justify-center gap-3 py-20">
              <Loader2 className="h-10 w-10 animate-spin text-slate-500" />
              <p className="text-sm text-muted-foreground">Carregando grupos pendentes...</p>
            </div>
          ) : (
            <div className="overflow-auto">
              <table className="w-full min-w-[860px] border-collapse text-sm">
                <thead className="sticky top-0 z-10 bg-slate-50">
                  <tr className="border-b">
                    <th className="w-12 px-4 py-3 text-left font-medium text-slate-700">Sel.</th>
                    <th className="px-4 py-3 text-left font-medium text-slate-700">Grupo</th>
                    <th className="px-4 py-3 text-left font-medium text-slate-700">Descricao</th>
                    <th className="px-4 py-3 text-left font-medium text-slate-700">NCM</th>
                    <th className="px-4 py-3 text-left font-medium text-slate-700">Codigos</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRows.map((row, index) => {
                    const key = getRowKey(row);
                    const checked = selectedKeys.includes(key);
                    return (
                      <tr key={index} className={`border-b align-top hover:bg-slate-50/70 ${checked ? "bg-blue-50/40" : ""}`}>
                        <td className="px-4 py-4">
                          <Checkbox checked={checked} onCheckedChange={(value) => toggleSelection(row, Boolean(value))} />
                        </td>
                        <td className="px-4 py-4 font-mono text-xs font-semibold text-slate-700">{normalizeValue(row.chave_produto)}</td>
                        <td className="px-4 py-4">
                          <div className="font-medium text-slate-900">{getRowDescription(row)}</div>
                          <div className="mt-1 text-xs text-slate-500">{normalizeValue(row.descricoes_conflitantes)}</div>
                        </td>
                        <td className="px-4 py-4 font-mono text-xs text-slate-600">{normalizeValue(row.ncm_consenso)}</td>
                        <td className="px-4 py-4 text-center font-mono text-xs text-slate-600">{normalizeValue(row.qtd_codigos)}</td>
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
            <div className="mt-1 text-xs text-slate-500">Selecione dois ou mais grupos. Para uniao, defina uma descricao canonica.</div>
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
