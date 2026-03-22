import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Loader2, Boxes, Info } from "lucide-react";
import { getProdutosDetalhesMulti, resolverManualUnificar, getNcmDetails, getCestDetails, type NcmDetailsResponse, type CestDetailsResponse } from "@/lib/pythonApi";
import { toast } from "sonner";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Label } from "@/components/ui/label";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";

interface MultiUnificarProdutosContentProps {
    cnpj: string;
    codigos: string[];
    onSuccess: () => void;
    onCancel: () => void;
}

export function MultiUnificarProdutosContent({
    cnpj,
    codigos,
    onSuccess,
    onCancel
}: MultiUnificarProdutosContentProps) {
    const [loading, setLoading] = useState(false);
    const [saving, setSaving] = useState(false);
    const [itens, setItens] = useState<any[]>([]);

    const [decisao, setDecisao] = useState({
        codigo: "",
        descricao: "",
        ncm: "",
        cest: "",
        gtin: "",
    });

    const [ncmCache, setNcmCache] = useState<Record<string, string>>({});
    const [cestCache, setCestCache] = useState<Record<string, string>>({});

    useEffect(() => {
        if (codigos && codigos.length > 0) {
            loadDetails();
        }
    }, [codigos, cnpj]);

    const getMinCodeWithSuffix = (codes: string[]) => {
        if (!codes.length) return "";
        const numericCodes = codes
            .map(c => c.replace(/\D/g, ""))
            .filter(c => c.length > 0)
            .map(c => parseInt(c, 10));
            
        if (numericCodes.length === 0) return `${codes[0]}_agr`;
        
        const min = Math.min(...numericCodes);
        return `${min}_agr`;
    };

    const loadDetails = async () => {
        setLoading(true);
        try {
            const res = await getProdutosDetalhesMulti(cnpj, codigos);
            if (res.success) {
                setItens(res.itens);
                
                const uniqueNcms = Array.from(new Set(res.itens.map((i: any) => i.ncm).filter(Boolean))) as string[];
                const uniqueCests = Array.from(new Set(res.itens.map((i: any) => i.cest).filter(Boolean))) as string[];
                
                // Fetch NCMs description
                Promise.all(uniqueNcms.map(n => getNcmDetails(n).catch(() => ({ success: false }))))
                    .then(results => {
                        const cache: Record<string, string> = {};
                        results.forEach((r: any, idx) => {
                            cache[uniqueNcms[idx]] = r?.success ? r.data.descricao : "NCM não localizado";
                        });
                        setNcmCache(prev => ({ ...prev, ...cache }));
                    });

                // Fetch CESTs description
                Promise.all(uniqueCests.map(c => getCestDetails(c).catch(() => ({ success: false }))))
                    .then(results => {
                        const cache: Record<string, string> = {};
                        results.forEach((r: any, idx) => {
                            cache[uniqueCests[idx]] = r?.success ? (r.data.descricoes?.[0] || "") : "CEST não localizado";
                        });
                        setCestCache(prev => ({ ...prev, ...cache }));
                    });

                if (res.itens && res.itens.length > 0) {
                    setDecisao({
                        codigo: getMinCodeWithSuffix(codigos),
                        descricao: (res.itens[0].descricao as string) || "",
                        ncm: (res.itens[0].ncm as string) || "",
                        cest: (res.itens[0].cest as string) || "",
                        gtin: (res.itens[0].gtin as string) || "",
                    });
                }
            }
        } catch (error) {
            toast.error("Erro ao carregar detalhes dos produtos.");
        } finally {
            setLoading(false);
        }
    };

    const handleConfirm = async () => {
        setSaving(true);
        try {
            const res = await resolverManualUnificar(cnpj, itens, decisao);
            if (res.status === "sucesso") {
                toast.success(res.mensagem);
                onSuccess();
            } else {
                toast.error(res.mensagem);
            }
        } catch (error) {
            toast.error("Erro ao salvar unificação.");
        } finally {
            setSaving(false);
        }
    };

    const getUniqueWithOptions = (key: string) => {
        const map = new Map<string, { sources: Set<string>, extra?: string, tipo_item?: string }>();
        itens.forEach(item => {
            const val = String(item[key] || "");
            if (val) {
                if (!map.has(val)) map.set(val, { sources: new Set() });
                const entry = map.get(val)!;
                if (item.fonte) entry.sources.add(item.fonte);
                
                if (key === "descricao" && item.descr_compl_c170) {
                    if (!entry.extra || String(item.descr_compl_c170).length > entry.extra.length) {
                        entry.extra = String(item.descr_compl_c170);
                    }
                }
                
                if (item.tipo_item) {
                    entry.tipo_item = String(item.tipo_item);
                }
            }
        });
        return Array.from(map.entries()).map(([value, info]) => ({
            value,
            sources: Array.from(info.sources).join(", "),
            extra: info.extra,
            tipo_item: info.tipo_item
        }));
    };

    const optionsDesc = getUniqueWithOptions("descricao");
    const optionsNcm = getUniqueWithOptions("ncm");
    const optionsCest = getUniqueWithOptions("cest");
    const optionsGtin = getUniqueWithOptions("gtin");

    if (loading) {
        return (
            <div className="flex flex-col items-center justify-center h-full gap-3 py-20">
                <Loader2 className="h-10 w-10 animate-spin text-blue-600" />
                <p className="text-sm font-black text-slate-600 uppercase tracking-widest">Carregando itens para agregação...</p>
            </div>
        );
    }

    return (
        <div className="flex flex-col h-full p-4 md:p-6">
            <div className="flex-1 overflow-hidden">
                <ScrollArea className="h-full pr-4">
                    <div className="flex flex-col gap-4 pb-6">
                        {/* Info Alert */}
                        <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg flex gap-3 text-amber-800">
                           <Info className="h-5 w-5 shrink-0 mt-0.5" />
                           <p className="text-xs leading-relaxed">
                             Várias identidades de produto foram selecionadas <strong>({codigos.length} códigos)</strong>. 
                             Selecione abaixo quais atributos devem prevalecer para o cadastro oficial unificado. 
                             Todas as linhas de origem ({itens.length}) serão vinculadas ao código de destino.
                           </p>
                        </div>

                        {/* Linha 1: Descrição */}
                        <div className="bg-slate-50/50 rounded-xl border border-slate-200 overflow-hidden shadow-inner">
                            <Label className="p-2.5 bg-white border-b text-blue-800 font-black text-[11px] uppercase tracking-widest flex items-center gap-2">
                                <div className="w-1.5 h-3 bg-blue-600 rounded-full" />
                                1. Seleção da Descrição Oficial
                            </Label>
                            <div className="p-3">
                                <RadioGroup
                                    value={decisao.descricao}
                                    onValueChange={(v) => setDecisao({ ...decisao, descricao: v })}
                                    className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3"
                                >
                                    {optionsDesc.map((opt, idx) => (
                                        <div key={idx} className={`flex items-start space-x-3 p-3 rounded-lg border transition-all cursor-pointer group hover:border-blue-300 ${decisao.descricao === opt.value ? "bg-blue-50 border-blue-400 shadow-sm" : "bg-white border-slate-200"}`}>
                                            <RadioGroupItem value={opt.value} id={`desc-${idx}`} className="mt-1 shrink-0" />
                                            <div className="flex flex-col gap-1 flex-1 min-w-0">
                                                <Label htmlFor={`desc-${idx}`} className="text-[12px] leading-snug cursor-pointer font-bold text-slate-900 line-clamp-2">
                                                    {opt.value}
                                                </Label>
                                                <div className="flex flex-wrap gap-1">
                                                    {opt.sources && (
                                                        <span className="py-0.5 px-1.5 bg-slate-100 text-slate-500 rounded-[3px] text-[9px] font-black uppercase tracking-tighter border border-slate-200">
                                                            {opt.sources}
                                                        </span>
                                                    )}
                                                    {opt.tipo_item && (
                                                        <span className="py-0.5 px-1.5 bg-blue-100 text-blue-600 rounded-[3px] text-[9px] font-black uppercase tracking-tighter border border-blue-200">
                                                            TIPO: {opt.tipo_item}
                                                        </span>
                                                    )}
                                                </div>
                                                {opt.extra && (
                                                    <span className="text-[10px] text-slate-500 italic leading-tight mt-1 bg-white/50 p-1.5 rounded-sm border border-slate-200/30">
                                                        {opt.extra}
                                                    </span>
                                                )}
                                            </div>
                                        </div>
                                    ))}
                                </RadioGroup>
                            </div>
                        </div>

                        {/* Linha 2: NCM */}
                        <div className="bg-slate-50/50 rounded-xl border border-slate-200 overflow-hidden shadow-inner">
                            <Label className="p-2.5 bg-white border-b text-blue-800 font-black text-[11px] uppercase tracking-widest flex items-center gap-2">
                                <div className="w-1.5 h-3 bg-blue-600 rounded-full" />
                                2. Seleção do NCM Consolidado
                            </Label>
                            <div className="p-3">
                                <RadioGroup
                                    value={decisao.ncm}
                                    onValueChange={(v) => setDecisao({ ...decisao, ncm: v })}
                                    className="grid grid-cols-1 md:grid-cols-2 gap-3"
                                >
                                    {optionsNcm.map((opt, idx) => (
                                        <div key={idx} className={`flex items-start space-x-3 p-3 rounded-lg border transition-all cursor-pointer group hover:border-blue-300 ${decisao.ncm === opt.value ? "bg-blue-50 border-blue-400 shadow-sm" : "bg-white border-slate-200"}`}>
                                            <RadioGroupItem value={opt.value} id={`ncm-${idx}`} className="mt-1 shrink-0" />
                                            <div className="flex flex-col gap-1 flex-1">
                                                <span className="text-[12px] font-black font-mono text-slate-900">{opt.value}</span>
                                                <span className="text-[10px] text-slate-500 font-medium leading-tight">
                                                    {ncmCache[opt.value] || "Carregando descrição..."}
                                                </span>
                                            </div>
                                        </div>
                                    ))}
                                </RadioGroup>
                            </div>
                        </div>

                        {/* Linha 3: CEST */}
                        <div className="bg-slate-50/50 rounded-xl border border-slate-200 overflow-hidden shadow-inner">
                            <Label className="p-2.5 bg-white border-b text-blue-800 font-black text-[11px] uppercase tracking-widest flex items-center gap-2">
                                <div className="w-1.5 h-3 bg-blue-600 rounded-full" />
                                3. Código CEST de Referência
                            </Label>
                            <div className="p-3">
                                <RadioGroup
                                    value={decisao.cest}
                                    onValueChange={(v) => setDecisao({ ...decisao, cest: v })}
                                    className="grid grid-cols-1 md:grid-cols-2 gap-3"
                                >
                                    {optionsCest.map((opt, idx) => (
                                        <div key={idx} className={`flex items-start space-x-3 p-3 rounded-lg border transition-all cursor-pointer group hover:border-purple-300 ${decisao.cest === opt.value ? "bg-purple-50 border-purple-400 shadow-sm" : "bg-white border-slate-200"}`}>
                                            <RadioGroupItem value={opt.value} id={`cest-${idx}`} className="mt-1 shrink-0" />
                                            <div className="flex flex-col gap-1 flex-1">
                                                <span className="text-[12px] font-black font-mono text-slate-900">{opt.value}</span>
                                                <span className="text-[10px] text-slate-500 font-medium leading-tight">
                                                    {cestCache[opt.value] || "Carregando descrição..."}
                                                </span>
                                            </div>
                                        </div>
                                    ))}
                                </RadioGroup>
                            </div>
                        </div>

                        {/* Linha 4: GTIN */}
                        <div className="bg-slate-50/50 rounded-xl border border-slate-200 overflow-hidden shadow-inner">
                            <Label className="p-2.5 bg-white border-b text-blue-800 font-black text-[11px] uppercase tracking-widest flex items-center gap-2">
                                <div className="w-1.5 h-3 bg-blue-600 rounded-full" />
                                4. Código GTIN (Barras)
                            </Label>
                            <div className="p-3">
                                <RadioGroup
                                    value={decisao.gtin}
                                    onValueChange={(v) => setDecisao({ ...decisao, gtin: v })}
                                    className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3"
                                >
                                    {optionsGtin.map((opt, idx) => (
                                        <div key={idx} className={`flex items-start space-x-2 p-2.5 rounded-lg border transition-all cursor-pointer group hover:border-slate-400 ${decisao.gtin === opt.value ? "bg-slate-100 border-slate-500 shadow-sm" : "bg-white border-slate-200"}`}>
                                            <RadioGroupItem value={opt.value} id={`gtin-${idx}`} className="mt-0.5 shrink-0" />
                                            <Label htmlFor={`gtin-${idx}`} className="text-[11px] font-black font-mono text-slate-800 truncate">
                                                {opt.value || "SEM GTIN"}
                                            </Label>
                                        </div>
                                    ))}
                                </RadioGroup>
                            </div>
                        </div>
                    </div>
                </ScrollArea>
            </div>

            <div className="mt-4 py-3 border-t flex justify-end gap-3 shrink-0 bg-slate-50/80 px-4 -mx-4 -mb-4 rounded-b-2xl">
                <Button variant="ghost" size="sm" onClick={onCancel} className="h-8 px-5 text-[10px] font-black uppercase tracking-widest text-slate-500 hover:text-slate-800">
                    Cancelar
                </Button>
                <Button
                    size="sm"
                    disabled={loading || saving}
                    onClick={handleConfirm}
                    className="h-10 px-12 bg-blue-600 hover:bg-blue-700 shadow-xl font-black text-[13px] uppercase tracking-widest transition-all rounded-lg"
                >
                    {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : "Consolidar Produtos"}
                </Button>
            </div>
        </div>
    );
}
