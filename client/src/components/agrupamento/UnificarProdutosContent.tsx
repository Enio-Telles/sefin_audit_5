import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Loader2 } from "lucide-react";
import {
  getCodigoMultiDescricaoResumo,
  getProdutoDetalhes,
  resolverManualUnificar,
  getNcmDetails,
  getCestDetails,
  type CodigoMultiDescricaoGrupoResumo,
  type CodigoMultiDescricaoOpcao,
  type NcmDetailsResponse,
  type CestDetailsResponse,
} from "@/lib/pythonApi";
import { toast } from "sonner";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Label } from "@/components/ui/label";
import { ScrollArea } from "@/components/ui/scroll-area";

interface UnificarProdutosContentProps {
  cnpj: string;
  codigo: string;
  onSuccess: () => void;
  onCancel: () => void;
  embedded?: boolean;
}

function normalizeText(value: unknown): string {
  return String(value ?? "").trim();
}

export function UnificarProdutosContent({
  cnpj,
  codigo,
  onSuccess,
  onCancel,
  embedded = false,
}: UnificarProdutosContentProps) {
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [resumo, setResumo] = useState<Record<string, unknown>>({});
  const [gruposDescricao, setGruposDescricao] = useState<CodigoMultiDescricaoGrupoResumo[]>([]);
  const [opcoesDescricao, setOpcoesDescricao] = useState<CodigoMultiDescricaoOpcao[]>([]);
  const [opcoesNcm, setOpcoesNcm] = useState<CodigoMultiDescricaoOpcao[]>([]);
  const [opcoesCest, setOpcoesCest] = useState<CodigoMultiDescricaoOpcao[]>([]);
  const [opcoesGtin, setOpcoesGtin] = useState<CodigoMultiDescricaoOpcao[]>([]);

  const [decisao, setDecisao] = useState({
    codigo: "",
    descricao: "",
    ncm: "",
    cest: "",
    gtin: "",
  });

  const [ncmCache, setNcmCache] = useState<Record<string, string>>({});
  const [cestCache, setCestCache] = useState<Record<string, string>>({});
  const [, setNcmDetails] = useState<NcmDetailsResponse["data"] | null>(null);
  const [, setCestDetails] = useState<CestDetailsResponse["data"] | null>(null);

  useEffect(() => {
    if (!decisao.ncm) {
      setNcmDetails(null);
      return;
    }
    getNcmDetails(decisao.ncm)
      .then((res) => (res.success ? setNcmDetails(res.data) : setNcmDetails(null)))
      .catch(() => setNcmDetails(null))
      .finally(() => undefined);
  }, [decisao.ncm]);

  useEffect(() => {
    if (!decisao.cest) {
      setCestDetails(null);
      return;
    }
    getCestDetails(decisao.cest)
      .then((res) => (res.success ? setCestDetails(res.data) : setCestDetails(null)))
      .catch(() => setCestDetails(null))
      .finally(() => undefined);
  }, [decisao.cest]);

  useEffect(() => {
    if (!codigo) return;
    void loadResumo();
  }, [codigo, cnpj]);

  const loadResumo = async () => {
    setLoading(true);
    try {
      const res = await getCodigoMultiDescricaoResumo(cnpj, codigo);
      if (!res.success) {
        setResumo({});
        setGruposDescricao([]);
        return;
      }

      setResumo(res.resumo || {});
      setGruposDescricao(res.grupos_descricao || []);
      setOpcoesDescricao(res.opcoes_consenso?.descricao || []);
      setOpcoesNcm(res.opcoes_consenso?.ncm || []);
      setOpcoesCest(res.opcoes_consenso?.cest || []);
      setOpcoesGtin(res.opcoes_consenso?.gtin || []);

      setDecisao({
        codigo,
        descricao: res.opcoes_consenso?.descricao?.[0]?.valor || "",
        ncm: res.opcoes_consenso?.ncm?.[0]?.valor || "",
        cest: res.opcoes_consenso?.cest?.[0]?.valor || "",
        gtin: res.opcoes_consenso?.gtin?.[0]?.valor || "",
      });

      const uniqueNcms = (res.opcoes_consenso?.ncm || []).map((item) => normalizeText(item.valor)).filter(Boolean);
      const uniqueCests = (res.opcoes_consenso?.cest || []).map((item) => normalizeText(item.valor)).filter(Boolean);

      Promise.all(
        uniqueNcms.map((ncm) => getNcmDetails(ncm).catch(() => ({ success: false, message: "Erro" as const })))
      ).then((results) => {
        const cache: Record<string, string> = {};
        results.forEach((result: any, index) => {
          cache[uniqueNcms[index]] = result?.success ? result.data.descricao : "NCM nao localizado";
        });
        setNcmCache((prev) => ({ ...prev, ...cache }));
      });

      Promise.all(
        uniqueCests.map((cest) => getCestDetails(cest).catch(() => ({ success: false, message: "Erro" as const })))
      ).then((results) => {
        const cache: Record<string, string> = {};
        results.forEach((result: any, index) => {
          cache[uniqueCests[index]] = result?.success ? result.data.descricoes?.[0] || "" : "CEST nao localizado";
        });
        setCestCache((prev) => ({ ...prev, ...cache }));
      });
    } catch {
      toast.error("Erro ao carregar resumo do codigo.");
    } finally {
      setLoading(false);
    }
  };

  const handleConfirm = async () => {
    setSaving(true);
    try {
      const detalhes = await getProdutoDetalhes(cnpj, codigo);
      if (!detalhes.success) {
        toast.error("Nao foi possivel carregar os detalhes brutos do codigo.");
        return;
      }
      const res = await resolverManualUnificar(cnpj, detalhes.itens, decisao);
      if (res.status === "sucesso") {
        toast.success(res.mensagem);
        onSuccess();
      } else {
        toast.error(res.mensagem);
      }
    } catch {
      toast.error("Erro ao salvar consolidacao.");
    } finally {
      setSaving(false);
    }
  };

  const resumoDescricaoMap = new Map(gruposDescricao.map((grupo) => [grupo.descricao, grupo]));

  const totalLinhas =
    Number(normalizeText(resumo.qtd_linhas)) ||
    gruposDescricao.reduce((acc, grupo) => acc + grupo.qtd_linhas, 0);
  const totalDescricoes = Number(normalizeText(resumo.qtd_descricoes)) || gruposDescricao.length;
  const totalGruposAfetados =
    Number(normalizeText(resumo.qtd_grupos_descricao_afetados)) || gruposDescricao.length;

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3 py-20">
        <Loader2 className="h-10 w-10 animate-spin text-blue-600" />
        <p className="text-sm font-black text-slate-600 uppercase tracking-widest">Sincronizando resumo do codigo...</p>
      </div>
    );
  }

  return (
    <div className={`flex flex-col h-full ${embedded ? "" : "p-4 md:p-6"}`}>
      <div className="flex-1 overflow-hidden">
        <ScrollArea className="h-full pr-4">
          <div className="flex flex-col gap-5 pb-6">
            <div className="rounded-xl border border-slate-200 bg-white px-4 py-3">
              <div className="grid gap-3 text-sm md:grid-cols-4">
                <div>
                  <div className="text-[10px] font-black uppercase tracking-widest text-slate-500">Codigo</div>
                  <div className="mt-1 font-mono font-black text-slate-900">{codigo}</div>
                </div>
                <div>
                  <div className="text-[10px] font-black uppercase tracking-widest text-slate-500">Descricoes</div>
                  <div className="mt-1 font-black text-slate-900">{totalDescricoes}</div>
                </div>
                <div>
                  <div className="text-[10px] font-black uppercase tracking-widest text-slate-500">Grupos afetados</div>
                  <div className="mt-1 font-black text-slate-900">{totalGruposAfetados}</div>
                </div>
                <div>
                  <div className="text-[10px] font-black uppercase tracking-widest text-slate-500">Linhas</div>
                  <div className="mt-1 font-black text-slate-900">{totalLinhas}</div>
                </div>
              </div>
            </div>

            <div className="space-y-5 rounded-xl border border-slate-200 bg-white p-4">
              <div>
                <div className="text-[11px] font-black uppercase tracking-widest text-slate-600">
                  Descricao canonica
                </div>
                <div className="mt-1 text-xs text-slate-500">
                  Escolha a descricao que representara este codigo.
                </div>
                <RadioGroup
                  value={decisao.descricao}
                  onValueChange={(value) => setDecisao({ ...decisao, descricao: value })}
                  className="mt-3 space-y-2"
                >
                  {opcoesDescricao.map((opt, index) => {
                    const grupo = resumoDescricaoMap.get(opt.valor);
                    return (
                      <label
                        key={index}
                        htmlFor={`desc-${index}`}
                        className={`flex cursor-pointer items-start gap-3 rounded-lg border px-3 py-2 ${
                          decisao.descricao === opt.valor
                            ? "border-blue-500 bg-blue-50"
                            : "border-slate-200 hover:border-slate-300"
                        }`}
                      >
                        <RadioGroupItem value={opt.valor} id={`desc-${index}`} className="mt-1 shrink-0" />
                        <div className="min-w-0 flex-1">
                          <div className="text-sm font-semibold leading-snug text-slate-900">{opt.valor}</div>
                          <div className="mt-1 text-xs text-slate-500">
                            {opt.qtd_linhas} linha{opt.qtd_linhas > 1 ? "s" : ""}
                            {grupo?.lista_tipo_item ? ` | tipo ${grupo.lista_tipo_item}` : ""}
                          </div>
                          {grupo?.lista_descr_compl ? (
                            <div className="mt-1 text-xs text-slate-500">{grupo.lista_descr_compl}</div>
                          ) : null}
                        </div>
                      </label>
                    );
                  })}
                </RadioGroup>
              </div>

              <div className="grid gap-5 md:grid-cols-2">
                <div>
                  <div className="text-[11px] font-black uppercase tracking-widest text-slate-600">NCM</div>
                  <RadioGroup
                    value={decisao.ncm}
                    onValueChange={(value) => setDecisao({ ...decisao, ncm: value })}
                    className="mt-3 space-y-2"
                  >
                    {opcoesNcm.length > 0 ? (
                      opcoesNcm.map((opt, index) => (
                        <label
                          key={index}
                          htmlFor={`ncm-${index}`}
                          className={`flex cursor-pointer items-start gap-3 rounded-lg border px-3 py-2 ${
                            decisao.ncm === opt.valor
                              ? "border-blue-500 bg-blue-50"
                              : "border-slate-200 hover:border-slate-300"
                          }`}
                        >
                          <RadioGroupItem value={opt.valor} id={`ncm-${index}`} className="mt-1 shrink-0" />
                          <div className="min-w-0 flex-1">
                            <div className="text-sm font-mono font-semibold text-slate-900">{opt.valor}</div>
                            <div className="text-xs text-slate-500">{opt.qtd_linhas} ocorrencias</div>
                            <div className="mt-1 text-xs text-slate-500">
                              {ncmCache[opt.valor] || "Carregando descricao..."}
                            </div>
                          </div>
                        </label>
                      ))
                    ) : (
                      <div className="rounded-lg border border-dashed border-slate-200 px-3 py-4 text-xs text-slate-500">
                        Nenhuma variacao de NCM encontrada.
                      </div>
                    )}
                  </RadioGroup>
                </div>

                <div>
                  <div className="text-[11px] font-black uppercase tracking-widest text-slate-600">CEST</div>
                  <RadioGroup
                    value={decisao.cest}
                    onValueChange={(value) => setDecisao({ ...decisao, cest: value })}
                    className="mt-3 space-y-2"
                  >
                    {opcoesCest.length > 0 ? (
                      opcoesCest.map((opt, index) => (
                        <label
                          key={index}
                          htmlFor={`cest-${index}`}
                          className={`flex cursor-pointer items-start gap-3 rounded-lg border px-3 py-2 ${
                            decisao.cest === opt.valor
                              ? "border-blue-500 bg-blue-50"
                              : "border-slate-200 hover:border-slate-300"
                          }`}
                        >
                          <RadioGroupItem value={opt.valor} id={`cest-${index}`} className="mt-1 shrink-0" />
                          <div className="min-w-0 flex-1">
                            <div className="text-sm font-mono font-semibold text-slate-900">{opt.valor}</div>
                            <div className="text-xs text-slate-500">{opt.qtd_linhas} ocorrencias</div>
                            <div className="mt-1 text-xs text-slate-500">
                              {cestCache[opt.valor] || "Carregando descricao..."}
                            </div>
                          </div>
                        </label>
                      ))
                    ) : (
                      <div className="rounded-lg border border-dashed border-slate-200 px-3 py-4 text-xs text-slate-500">
                        Nenhuma variacao de CEST encontrada.
                      </div>
                    )}
                  </RadioGroup>
                </div>
              </div>

              <div>
                <div className="text-[11px] font-black uppercase tracking-widest text-slate-600">GTIN</div>
                <RadioGroup
                  value={decisao.gtin}
                  onValueChange={(value) => setDecisao({ ...decisao, gtin: value })}
                  className="mt-3 space-y-2"
                >
                  {opcoesGtin.length > 0 ? (
                    opcoesGtin.map((opt, index) => (
                      <label
                        key={index}
                        htmlFor={`gtin-${index}`}
                        className={`flex cursor-pointer items-start gap-3 rounded-lg border px-3 py-2 ${
                          decisao.gtin === opt.valor
                            ? "border-blue-500 bg-blue-50"
                            : "border-slate-200 hover:border-slate-300"
                        }`}
                      >
                        <RadioGroupItem value={opt.valor} id={`gtin-${index}`} className="mt-1 shrink-0" />
                        <div className="min-w-0 flex-1">
                          <div className="text-sm font-mono font-semibold text-slate-900">
                            {opt.valor || "SEM GTIN"}
                          </div>
                          <div className="text-xs text-slate-500">{opt.qtd_linhas} ocorrencias</div>
                        </div>
                      </label>
                    ))
                  ) : (
                    <div className="rounded-lg border border-dashed border-slate-200 px-3 py-4 text-xs text-slate-500">
                      Nenhum GTIN disponivel.
                    </div>
                  )}
                </RadioGroup>
              </div>
            </div>
          </div>
        </ScrollArea>
      </div>

      <div className={`mt-4 border-t bg-slate-50/80 px-4 py-3 -mx-4 -mb-4 flex items-center justify-between gap-3 shrink-0 ${embedded ? "rounded-b-none" : "rounded-b-2xl"}`}>
        <div className="text-xs text-slate-500">
          A consolidacao mantera o codigo e aplicara uma descricao e atributos de consenso.
        </div>
        <div className="flex justify-end gap-3">
        <Button variant="ghost" size="sm" onClick={onCancel} className="h-8 px-5 text-[10px] font-black uppercase tracking-widest text-slate-500 hover:text-slate-800">
          Voltar
        </Button>
        <Button size="sm" disabled={loading || saving} onClick={handleConfirm} className="h-10 px-12 bg-blue-600 hover:bg-blue-700 shadow-xl font-black text-[13px] uppercase tracking-widest transition-all hover:scale-[1.02] active:scale-[0.98] rounded-lg">
          {saving ? (
            <>
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              Gravando...
            </>
          ) : "Confirmar consolidacao do codigo"}
        </Button>
        </div>
      </div>
    </div>
  );
}
