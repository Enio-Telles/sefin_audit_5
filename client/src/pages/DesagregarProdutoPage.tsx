import { useParams } from "wouter";
import { DesagregarProdutosContent } from "@/components/agrupamento/DesagregarProdutosContent";
import { ChevronLeft, SplitSquareHorizontal } from "lucide-react";
import { Button } from "@/components/ui/button";

export default function DesagregarProdutoPage() {
  const { cnpj, codigo } = useParams();

  if (!cnpj || !codigo) {
    return (
      <div className="flex h-screen items-center justify-center bg-slate-50">
        <p className="font-bold uppercase tracking-widest text-slate-500">Parametros invalidos.</p>
      </div>
    );
  }

  const handleUpdated = () => {
    if (window.opener && !window.opener.closed) {
      window.opener.postMessage(
        {
          type: "produto-revisao-atualizada",
          cnpj,
          codigo,
        },
        window.location.origin
      );
    }
    window.close();
  };

  return (
    <div className="min-h-screen bg-slate-50 flex flex-col overflow-hidden">
      <header className="shrink-0 border-b border-slate-200 bg-white px-5 py-3">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={() => window.close()} className="rounded-lg hover:bg-slate-100" aria-label="Voltar" title="Voltar">
            <ChevronLeft className="h-5 w-5 text-slate-600" />
          </Button>
          <div className="flex items-center gap-3">
            <SplitSquareHorizontal className="h-5 w-5 text-purple-600" />
            <div>
              <h1 className="text-base font-black uppercase tracking-tight text-slate-900 leading-none">
                Separar codigo
              </h1>
              <p className="mt-1 text-[11px] font-medium text-slate-500">
                Codigo {codigo} | CNPJ {cnpj}
              </p>
            </div>
          </div>
        </div>
      </header>

      <main className="flex-1 overflow-hidden p-4 md:p-5">
        <div className="mx-auto h-full max-w-[1600px] overflow-hidden rounded-xl border border-slate-200 bg-white">
          <DesagregarProdutosContent
            cnpj={cnpj}
            codigo={codigo}
            onSuccess={handleUpdated}
            onCancel={() => window.close()}
          />
        </div>
      </main>
    </div>
  );
}
