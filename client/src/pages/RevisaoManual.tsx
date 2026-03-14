import { useEffect, useMemo, useState } from "react";
import { useLocation } from "wouter";
import {
  AlertCircle,
  ArrowUpDown,
  Boxes,
  ChevronLeft,
  GitMerge,
  Loader2,
  RefreshCw,
  SplitSquareHorizontal,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { toast } from "sonner";
import { getCodigosMultiDescricao } from "@/lib/pythonApi";

type ReviewRow = Record<string, unknown>;

function normalizeValue(value: unknown): string {
  return String(value ?? "").trim();
}

function splitDescriptions(value: unknown): string[] {
  return normalizeValue(value)
    .split("<<#>>")
    .map((item) => item.trim())
    .filter(Boolean);
}

function sumMetric(rows: ReviewRow[], field: string): number {
  return rows.reduce((acc, row) => acc + Number(normalizeValue(row[field]) || 0), 0);
}

function StatBox({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-lg border border-slate-200 bg-white px-4 py-3">
      <div className="text-[10px] font-black uppercase tracking-widest text-slate-500">{label}</div>
      <div className="mt-1 text-2xl font-black text-slate-900">{value}</div>
    </div>
  );
}

export default function RevisaoManual() {
  const [, navigate] = useLocation();
  const searchParams = new URLSearchParams(window.location.search);
  const cnpj = searchParams.get("cnpj") || "";

  const [loading, setLoading] = useState(true);
  const [rows, setRows] = useState<ReviewRow[]>([]);
  const [sortColumn, setSortColumn] = useState<string | undefined>("qtd_descricoes");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc" | undefined>("desc");

  const loadData = async () => {
    if (!cnpj) {
      setRows([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      const res = await getCodigosMultiDescricao(cnpj);
      setRows(res.success ? res.data : []);
    } catch (error) {
      console.error("Erro ao carregar codigos multidescricao:", error);
      toast.error("Erro ao carregar revisao residual", {
        description: "Nao foi possivel carregar a tabela residual de codigos com multiplas descricoes.",
      });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadData();
  }, [cnpj]);

  const handleSort = (column: string) => {
    if (sortColumn === column) {
      setSortDirection((prev) => (prev === "asc" ? "desc" : prev === "desc" ? undefined : "asc"));
      if (sortDirection === "desc") {
        setSortColumn(undefined);
      }
      return;
    }
    setSortColumn(column);
    setSortDirection("asc");
  };

  const sortedRows = useMemo(() => {
    const data = [...rows];
    if (!sortColumn || !sortDirection) return data;

    return data.sort((a, b) => {
      const aVal = normalizeValue(a[sortColumn]);
      const bVal = normalizeValue(b[sortColumn]);
      const aNum = Number(aVal);
      const bNum = Number(bVal);
      const bothNumeric = Number.isFinite(aNum) && Number.isFinite(bNum);
      const cmp = bothNumeric ? aNum - bNum : aVal.localeCompare(bVal);
      return sortDirection === "asc" ? cmp : -cmp;
    });
  }, [rows, sortColumn, sortDirection]);

  const totalDescricoes = useMemo(() => sumMetric(rows, "qtd_descricoes"), [rows]);
  const totalGrupos = useMemo(() => sumMetric(rows, "qtd_grupos_descricao_afetados"), [rows]);

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
        <StatBox label="Codigos pendentes" value={rows.length} />
        <StatBox label="Descricoes envolvidas" value={totalDescricoes} />
        <StatBox label="Grupos afetados" value={totalGrupos} />
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
            <div className="text-xs font-semibold text-slate-500">{sortedRows.length} pendentes</div>
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
                        {splitDescriptions(row.lista_descricoes).map((desc, descIndex) => (
                          <div key={descIndex} className={descIndex > 0 ? "border-t pt-2 text-slate-500" : "font-medium"}>
                            {desc}
                          </div>
                        ))}
                      </div>
                      <div className="mt-2 text-xs text-slate-500">Grupos: {normalizeValue(row.lista_chave_produto)}</div>
                    </td>
                    <td className="px-4 py-4 text-center font-mono text-xs text-slate-600">{normalizeValue(row.qtd_descricoes)}</td>
                    <td className="px-4 py-4 text-center font-mono text-xs text-slate-600">{normalizeValue(row.qtd_grupos_descricao_afetados)}</td>
                    <td className="px-4 py-4 text-xs text-slate-600">{normalizeValue(row.lista_ncm)}</td>
                    <td className="px-4 py-4 text-xs text-slate-600">{normalizeValue(row.lista_cest)}</td>
                    <td className="px-4 py-4 text-xs text-slate-600">{normalizeValue(row.lista_gtin)}</td>
                    <td className="sticky right-0 bg-white px-4 py-4">
                      <div className="flex justify-center gap-2">
                        <Button size="sm" variant="outline" className="gap-1.5" onClick={() => window.open(`/unificar/${cnpj}/${normalizeValue(row.codigo)}`, "_blank")}>
                          <Boxes className="h-3.5 w-3.5" />
                          Consolidar
                        </Button>
                        <Button size="sm" variant="outline" className="gap-1.5" onClick={() => window.open(`/desagregar/${cnpj}/${normalizeValue(row.codigo)}`, "_blank")}>
                          <SplitSquareHorizontal className="h-3.5 w-3.5" />
                          Separar
                        </Button>
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
