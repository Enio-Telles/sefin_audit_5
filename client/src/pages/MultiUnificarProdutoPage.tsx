import { useRoute } from "wouter";
import { MultiUnificarProdutosContent } from "@/components/agrupamento/MultiUnificarProdutosContent";
import { Boxes, ChevronLeft } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";

export default function MultiUnificarProdutoPage() {
  const [, params] = useRoute("/unificar-multi/:cnpj");
  const searchParams = new URLSearchParams(window.location.search);
  const codigosStr = searchParams.get("codigos") || "";
  const codigos = codigosStr ? codigosStr.split(",") : [];

  if (!params?.cnpj || codigos.length === 0) {
    return (
      <div className="flex items-center justify-center h-screen">
        <p className="text-slate-500 font-bold uppercase tracking-widest">
          Nenhum produto selecionado para unificação
        </p>
      </div>
    );
  }

  const { cnpj } = params;

  return (
    <div className="min-h-screen bg-slate-100 flex flex-col overflow-hidden">
      {/* Header fixo */}
      <header className="bg-white border-b border-slate-200 px-6 py-4 flex items-center justify-between shadow-sm z-10 shrink-0">
        <div className="flex items-center gap-4">
          <Button
            variant="ghost"
            size="icon"
            onClick={() => window.close()}
            className="hover:bg-slate-100 rounded-full"
            aria-label="Voltar"
            title="Voltar"
          >
            <ChevronLeft className="h-5 w-5 text-slate-500" />
          </Button>
          <div className="flex items-center gap-3">
            <Boxes className="h-6 w-6 text-blue-600" />
            <div>
              <h1 className="text-xl font-black text-slate-900 tracking-tight uppercase leading-none">
                Consolidação Manual: {codigos.length} Itens
              </h1>
              <p className="text-[10px] text-slate-500 font-bold uppercase tracking-widest mt-1">
                Unificação de múltiplos registros em um único cadastro oficial •
                CNPJ {cnpj}
              </p>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <Badge
            variant="outline"
            className="px-3 py-1 bg-blue-50 text-blue-700 border-blue-100 text-[10px] font-black uppercase tracking-widest"
          >
            Revisão Final
          </Badge>
        </div>
      </header>

      {/* Conteúdo Principal */}
      <main className="flex-1 overflow-hidden p-6">
        <div className="max-w-[1800px] mx-auto h-full bg-white rounded-2xl shadow-2xl border border-slate-200 overflow-hidden">
          <MultiUnificarProdutosContent
            cnpj={cnpj}
            codigos={codigos}
            onSuccess={() => {
              if (window.opener && !window.opener.closed) {
                window.opener.postMessage(
                  {
                    type: "produto-consolidacao-concluida",
                    cnpj,
                    codigos,
                  },
                  window.location.origin
                );
              }
              window.close();
            }}
            onCancel={() => window.close()}
          />
        </div>
      </main>
    </div>
  );
}
