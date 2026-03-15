import { useState, useEffect } from "react";
import { useLocation } from "wouter";
import { 
  Boxes, 
  Search, 
  ArrowUpDown, 
  Loader2,
  ChevronLeft,
  FileSpreadsheet,
  CheckCircle2,
  Info,
  MousePointerClick,
  Filter,
  X
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Checkbox } from "@/components/ui/checkbox";
import { toast } from "sonner";
import {
  desfazerProdutoVerificado,
  getStatusAnaliseProdutos,
  marcarProdutoVerificado,
  readParquet,
  type ParquetReadResponse,
  type ProdutoAnaliseStatusResumo,
  type ProdutoAnaliseStatusItem,
} from "@/lib/pythonApi";

const DESCRIPTION_SEPARATOR = "<<#>>";

export default function AgregacaoSelecao() {
  const [location, navigate] = useLocation();
  const searchParams = new URLSearchParams(window.location.search);
  const cnpj = searchParams.get("cnpj") || "";
  const filePath = searchParams.get("file_path") || "";
  const storageKey = `produto-consolidacao-selecao-ui:${cnpj}:${filePath}`;

  const [loading, setLoading] = useState(true);
  const [data, setData] = useState<ParquetReadResponse | null>(null);
  const [statusRows, setStatusRows] = useState<ProdutoAnaliseStatusItem[]>([]);
  const [statusResumo, setStatusResumo] = useState<ProdutoAnaliseStatusResumo | null>(null);
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [searchInput, setSearchInput] = useState({
    chave_produto: "",
    lista_descricao: ""
  });
  const [sortColumn, setSortColumn] = useState<string | undefined>(undefined);
  const [sortDirection, setSortDirection] = useState<"asc" | "desc" | undefined>(undefined);

  const [selectedCodigos, setSelectedCodigos] = useState<Set<string>>(new Set());
  const [statusSaving, setStatusSaving] = useState(false);
  const [showVerified, setShowVerified] = useState(false);

  const resetUiState = () => {
    setSearchInput({ chave_produto: "", lista_descricao: "" });
    setFilters({});
    setSortColumn(undefined);
    setSortDirection(undefined);
    setShowVerified(false);
    window.sessionStorage.removeItem(storageKey);
  };

  useEffect(() => {
    if (!cnpj || !filePath) return;
    try {
      const raw = window.sessionStorage.getItem(storageKey);
      if (!raw) return;
      const state = JSON.parse(raw) as {
        searchInput?: { chave_produto?: string; lista_descricao?: string };
        sortColumn?: string;
        sortDirection?: "asc" | "desc";
        showVerified?: boolean;
      };
      if (state.searchInput) {
        setSearchInput({
          chave_produto: state.searchInput.chave_produto || "",
          lista_descricao: state.searchInput.lista_descricao || "",
        });
      }
      if (typeof state.sortColumn === "string") setSortColumn(state.sortColumn);
      if (state.sortDirection === "asc" || state.sortDirection === "desc") setSortDirection(state.sortDirection);
      if (typeof state.showVerified === "boolean") setShowVerified(state.showVerified);
    } catch {
      // ignore invalid session state
    }
  }, [cnpj, filePath, storageKey]);

  useEffect(() => {
    if (!cnpj || !filePath) return;
    window.sessionStorage.setItem(
      storageKey,
      JSON.stringify({
        searchInput,
        sortColumn,
        sortDirection,
        showVerified,
      })
    );
  }, [cnpj, filePath, storageKey, searchInput, sortColumn, sortDirection, showVerified]);

  useEffect(() => {
    if (filePath) {
      loadData();
    }
  }, [filePath, filters, sortColumn, sortDirection]);

  useEffect(() => {
    const handleConsolidacaoConcluida = (event: MessageEvent) => {
      if (event.origin !== window.location.origin) {
        return;
      }

      if (event.data?.type !== "produto-consolidacao-concluida") {
        return;
      }

      if (event.data?.cnpj !== cnpj) {
        return;
      }

      setSelectedCodigos(new Set());
      loadData();
    };

    window.addEventListener("message", handleConsolidacaoConcluida);
    return () => window.removeEventListener("message", handleConsolidacaoConcluida);
  }, [cnpj, filePath, filters, sortColumn, sortDirection]);

  const loadData = async () => {
    setLoading(true);
    try {
      const [res, statusRes] = await Promise.all([
        readParquet({
          file_path: filePath,
          page: 1,
          page_size: 2000,
          filters,
          sort_column: sortColumn,
          sort_direction: sortDirection
        }),
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
      setData(res);
      setStatusRows(statusRes.success ? statusRes.data : []);
      setStatusResumo(statusRes.success ? statusRes.resumo : null);
    } catch (error) {
      console.error("Erro ao carregar dados para agregação:", error);
      toast.error("Erro ao carregar dados", {
        description: "Não foi possível carregar a tabela de produtos."
      });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    const timer = setTimeout(() => {
        const newFilters: Record<string, string> = {};
        if (searchInput.chave_produto) newFilters.chave_produto = searchInput.chave_produto;
        if (searchInput.lista_descricao) newFilters.lista_descricao = searchInput.lista_descricao;
        setFilters(newFilters);
    }, 500);
    return () => clearTimeout(timer);
  }, [searchInput]);

  const clearFilters = () => {
    setSearchInput({ chave_produto: "", lista_descricao: "" });
    setFilters({});
  };

  const handleSort = (column: string) => {
    if (sortColumn === column) {
      setSortDirection(prev => prev === "asc" ? "desc" : prev === "desc" ? undefined : "asc");
      if (sortDirection === "desc") setSortColumn(undefined);
    } else {
      setSortColumn(column);
      setSortDirection("asc");
    }
  };

  const verifiedByGrupo = new Set(
    statusRows
      .filter((item) => item.tipo_ref === "POR_GRUPO" && item.status_analise === "VERIFICADO_SEM_ACAO")
      .map((item) => String(item.ref_id))
  );

  const filteredRows = (data?.rows || []).filter((row) =>
    showVerified ? true : !verifiedByGrupo.has(String(row.chave_produto))
  );

  const toggleSelection = (codigo: string) => {
    const next = new Set(selectedCodigos);
    if (next.has(codigo)) {
      next.delete(codigo);
    } else {
      next.add(codigo);
    }
    setSelectedCodigos(next);
  };

  const toggleAll = () => {
    if (selectedCodigos.size === filteredRows.length) {
      setSelectedCodigos(new Set());
    } else {
      setSelectedCodigos(new Set(filteredRows.map(r => String(r.chave_produto))));
    }
  };

  const handleAggregateSelected = () => {
    if (selectedCodigos.size < 1) {
      toast.error("Selecione ao menos um produto para agregar.");
      return;
    }
    const codigosArr = Array.from(selectedCodigos);
    const url = `/unificar-multi/${cnpj}?codigos=${encodeURIComponent(codigosArr.join(","))}`;
    const popup = window.open(url, "_blank");

    if (!popup) {
      toast.error("Não foi possível abrir a janela de consolidação.");
      return;
    }
  };

  const handleMarkSelectedVerified = async () => {
    if (selectedCodigos.size === 0) {
      toast.error("Selecione ao menos um produto.");
      return;
    }

    setStatusSaving(true);
    try {
      const selectedRows = filteredRows.filter((row) => selectedCodigos.has(String(row.chave_produto)));
      await Promise.all(
        selectedRows.map((row) =>
          marcarProdutoVerificado({
            cnpj,
            tipo_ref: "POR_GRUPO",
            ref_id: String(row.chave_produto),
            descricao_ref: String(row.descricao || row.lista_descricao || ""),
            contexto_tela: "CONSOLIDACAO_SELECAO",
          })
        )
      );
      setSelectedCodigos(new Set());
      toast.success("Produtos marcados como verificados.");
      await loadData();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Erro ao marcar produtos como verificados.");
    } finally {
      setStatusSaving(false);
    }
  };

  const handleUnverifySelected = async () => {
    if (selectedCodigos.size === 0) {
      toast.error("Selecione ao menos um produto.");
      return;
    }

    setStatusSaving(true);
    try {
      const selectedRows = filteredRows.filter((row) => selectedCodigos.has(String(row.chave_produto)));
      await Promise.all(
        selectedRows.map((row) =>
          desfazerProdutoVerificado({
            cnpj,
            tipo_ref: "POR_GRUPO",
            ref_id: String(row.chave_produto),
            descricao_ref: String(row.descricao || row.lista_descricao || ""),
            contexto_tela: "CONSOLIDACAO_SELECAO",
          })
        )
      );
      setSelectedCodigos(new Set());
      toast.success("Marcacao de verificado removida.");
      await loadData();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Erro ao desfazer verificado.");
    } finally {
      setStatusSaving(false);
    }
  };

  if (!filePath) {
    return (
      <div className="flex flex-col items-center justify-center h-[70vh] gap-4">
        <Info className="h-12 w-12 text-muted-foreground" />
        <h2 className="text-xl font-semibold">Caminho do arquivo não encontrado</h2>
        <p className="text-muted-foreground">Esta página deve ser aberta a partir da visualização de uma auditoria.</p>
        <Button onClick={() => navigate("/auditar")}>Ir para Auditoria</Button>
      </div>
    );
  }

  return (
    <div className="container mx-auto py-6 space-y-6">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div className="space-y-1">
          <div className="flex items-center gap-2">
            <Button variant="ghost" size="icon" className="h-8 w-8" onClick={() => window.history.back()} aria-label="Voltar" title="Voltar">
              <ChevronLeft className="h-4 w-4" />
            </Button>
            <h1 className="text-2xl font-bold tracking-tight text-slate-800">Consolidacao por Selecao</h1>
          </div>
          <p className="text-muted-foreground flex items-center gap-2">
            <Badge variant="outline" className="font-mono bg-slate-50">{cnpj}</Badge>
            <span>Selecione livremente produtos para consolidar em um único cadastro.</span>
          </p>
        </div>
        
        <div className="flex items-center gap-2">
           <Button
            variant="outline"
            size="sm"
            className="gap-2 h-9"
            onClick={resetUiState}
           >
             Restaurar padrao
           </Button>
           <Button
            variant={showVerified ? "default" : "outline"}
            size="sm"
            className="gap-2 h-9"
            onClick={() => setShowVerified((prev) => !prev)}
           >
             <CheckCircle2 className="h-4 w-4" />
             {showVerified ? "Ocultar verificados" : "Mostrar verificados"}
           </Button>
           <Button
            variant="outline"
            size="sm"
            className="gap-2 h-9"
            onClick={handleMarkSelectedVerified}
            disabled={selectedCodigos.size === 0 || statusSaving}
           >
             {statusSaving ? <Loader2 className="h-4 w-4 animate-spin" /> : <CheckCircle2 className="h-4 w-4" />}
             Verificados ({selectedCodigos.size})
           </Button>
           <Button
            variant="outline"
            size="sm"
            className="gap-2 h-9"
            onClick={handleUnverifySelected}
            disabled={selectedCodigos.size === 0 || statusSaving}
           >
             {statusSaving ? <Loader2 className="h-4 w-4 animate-spin" /> : <X className="h-4 w-4" />}
             Desfazer verificado
           </Button>
           <Button 
            variant="default" 
            size="sm" 
            className="gap-2 bg-blue-600 hover:bg-blue-700 h-9 font-bold uppercase tracking-wider px-6 shadow-md"
            onClick={handleAggregateSelected}
            disabled={selectedCodigos.size === 0}
           >
             <Boxes className="h-4 w-4" />
             Consolidar Selecionados ({selectedCodigos.size})
           </Button>
           <Button variant="outline" size="icon" className="h-9 w-9" onClick={() => loadData()} aria-label="Atualizar dados" title="Atualizar dados">
             <Loader2 className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
           </Button>
        </div>
      </div>

      <Separator />

      <div className="grid gap-3 md:grid-cols-5">
        <div className="rounded-lg border border-slate-200 bg-white px-4 py-3">
          <div className="text-[10px] font-black uppercase tracking-widest text-slate-500">Pendentes</div>
          <div className="mt-1 text-2xl font-black text-slate-900">{statusResumo?.pendentes ?? 0}</div>
        </div>
        <div className="rounded-lg border border-slate-200 bg-white px-4 py-3">
          <div className="text-[10px] font-black uppercase tracking-widest text-slate-500">Verificados</div>
          <div className="mt-1 text-2xl font-black text-slate-900">{statusResumo?.verificados ?? 0}</div>
        </div>
        <div className="rounded-lg border border-indigo-300 bg-indigo-50 px-4 py-3 shadow-sm">
          <div className="text-[10px] font-black uppercase tracking-widest text-indigo-700">Consolidados</div>
          <div className="mt-1 text-2xl font-black text-indigo-900">{statusResumo?.consolidados ?? 0}</div>
        </div>
        <div className="rounded-lg border border-slate-200 bg-white px-4 py-3">
          <div className="text-[10px] font-black uppercase tracking-widest text-slate-500">Separados</div>
          <div className="mt-1 text-2xl font-black text-slate-900">{statusResumo?.separados ?? 0}</div>
        </div>
        <div className="rounded-lg border border-slate-200 bg-white px-4 py-3">
          <div className="text-[10px] font-black uppercase tracking-widest text-slate-500">Decididos entre grupos</div>
          <div className="mt-1 text-2xl font-black text-slate-900">{statusResumo?.decididos_entre_grupos ?? 0}</div>
        </div>
      </div>

      {/* Filtros */}
      <Card className="border-slate-200 shadow-sm">
        <CardContent className="p-4 flex flex-wrap items-end gap-4">
          <div className="flex-1 min-w-[200px] space-y-1.5">
            <Label className="text-[10px] font-black uppercase tracking-widest text-slate-500">Filtrar por Código</Label>
            <div className="relative">
              <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
              <Input 
                placeholder="Ex: 100..." 
                className="pl-9 h-9 border-slate-200"
                value={searchInput.chave_produto}
                onChange={(e) => setSearchInput(prev => ({ ...prev, chave_produto: e.target.value }))}
              />
            </div>
          </div>
          <div className="flex-[2] min-w-[300px] space-y-1.5">
            <Label className="text-[10px] font-black uppercase tracking-widest text-slate-500">Filtrar por Descrição</Label>
            <div className="relative">
              <Filter className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
              <Input 
                placeholder="Trecho da descrição..." 
                className="pl-9 h-9 border-slate-200"
                value={searchInput.lista_descricao}
                onChange={(e) => setSearchInput(prev => ({ ...prev, lista_descricao: e.target.value }))}
              />
            </div>
          </div>
          <Button variant="ghost" size="sm" className="h-9 px-3 gap-2 text-slate-500" onClick={clearFilters}>
            <X className="h-4 w-4" />
            Limpar
          </Button>
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 gap-6">
        <Card className="border-blue-100 shadow-sm overflow-hidden">
          <CardHeader className="bg-slate-50/80 pb-4 border-b">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <MousePointerClick className="h-5 w-5 text-blue-600" />
                <CardTitle className="text-lg">Catálogo Geral de Produtos</CardTitle>
              </div>
              <Badge variant="secondary" className="bg-blue-100 text-blue-800 hover:bg-blue-100 uppercase text-[10px] font-bold tracking-wider">
                {filteredRows.length} Itens Carregados
              </Badge>
            </div>
            <CardDescription>
              Marque os produtos que deseja unificar. Todas as linhas de origem dos produtos selecionados serão carregadas para a decisão final.
            </CardDescription>
          </CardHeader>
          <CardContent className="p-0 overflow-auto max-h-[70vh]">
            {loading && filteredRows.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-20 gap-3">
                <Loader2 className="h-10 w-10 animate-spin text-blue-600" />
                <p className="text-muted-foreground animate-pulse text-sm">Buscando catálogo de produtos...</p>
              </div>
            ) : filteredRows.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-20 gap-3 text-center px-4">
                <h3 className="text-lg font-medium text-slate-800">Nenhum produto encontrado.</h3>
              </div>
            ) : (
              <table className="w-full text-sm border-collapse">
                <thead className="bg-slate-50/50 sticky top-0 z-10 border-b">
                  <tr>
                    <th className="px-4 py-3 w-10">
                      <Checkbox 
                        checked={selectedCodigos.size === filteredRows.length && filteredRows.length > 0}
                        onCheckedChange={toggleAll}
                      />
                    </th>
                    {[
                      { key: "chave_produto", label: "CÓDIGO" },
                      { key: "lista_descricao", label: "DESCRIÇÕES" },
                      { key: "qtd_descricoes", label: "QTD. DESC." },
                      { key: "qtd_codigos", label: "QTD. COD." },
                      { key: "ncm_consenso", label: "NCM" },
                      { key: "lista_unid", label: "UNIDADES" }
                    ].map(col => (
                      <th key={col.key} className="px-4 py-3 text-left font-semibold text-slate-700">
                        <button className="flex items-center gap-1 hover:text-blue-600 transition-colors" onClick={() => handleSort(col.key)}>
                          {col.label}
                          {sortColumn === col.key ? (
                            <ArrowUpDown className={`h-3 w-3 ${sortDirection ? 'text-blue-600' : 'text-slate-300'}`} />
                          ) : (
                            <ArrowUpDown className="h-3 w-3 text-slate-300 opacity-0 group-hover:opacity-100" />
                          )}
                        </button>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {filteredRows.map((row, idx) => {
                    const codigo = String(row.chave_produto);
                    const isSelected = selectedCodigos.has(codigo);
                    const descricoes = String(row.lista_descricao || "").split(DESCRIPTION_SEPARATOR).map((desc) => desc.trim()).filter(Boolean);
                    return (
                      <tr 
                        key={idx} 
                        className={`hover:bg-blue-50/30 transition-colors group cursor-pointer ${isSelected ? 'bg-blue-50/50' : ''}`}
                        onClick={() => toggleSelection(codigo)}
                      >
                        <td className="px-4 py-3" onClick={(e) => e.stopPropagation()}>
                           <Checkbox 
                             checked={isSelected}
                             onCheckedChange={() => toggleSelection(codigo)}
                           />
                        </td>
                        <td className="px-4 py-3 font-mono text-xs font-bold text-slate-600">{codigo}</td>
                        <td className="px-4 py-3 max-w-md text-slate-800">
                          <div className="flex flex-col gap-1">
                            {descricoes.slice(0, 2).map((desc, i) => (
                              <span key={i} className={i > 0 ? "text-[10px] text-muted-foreground" : "font-medium"}>
                                {desc}
                              </span>
                            ))}
                            {descricoes.length > 2 && (
                                <span className="text-[9px] text-blue-500 font-bold italic">+{descricoes.length - 2} mais...</span>
                            )}
                          </div>
                        </td>
                        <td className="px-4 py-3 font-mono text-xs text-slate-600 text-center">{String(row.qtd_descricoes ?? "")}</td>
                        <td className="px-4 py-3 font-mono text-xs text-slate-600 text-center">{String(row.qtd_codigos ?? "")}</td>
                        <td className="px-4 py-3 font-mono text-xs text-slate-500">{row.ncm_consenso as string}</td>
                        <td className="px-4 py-3">
                          <div className="flex flex-wrap gap-1">
                            {String(row.lista_unid || "").split(", ").map((un, i) => (
                              <Badge key={i} variant="outline" className="text-[9px] uppercase font-bold text-slate-400 bg-white h-4 px-1">
                                {un}
                              </Badge>
                            ))}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
