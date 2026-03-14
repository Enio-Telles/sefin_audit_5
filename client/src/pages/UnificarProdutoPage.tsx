import { useRoute } from "wouter";
import { UnificarProdutosContent } from "@/components/agrupamento/UnificarProdutosContent";
import { Boxes, ChevronLeft } from "lucide-react";
import { Button } from "@/components/ui/button";

export default function UnificarProdutoPage() {
  const [, params] = useRoute("/unificar/:cnpj/:codigo");

  if (!params?.cnpj || !params?.codigo) {
    return (
      <div className="flex h-screen items-center justify-center bg-slate-50">
        <p className="font-bold uppercase tracking-widest text-slate-500">Parametros invalidos</p>
      </div>
    );
  }

  const { cnpj, codigo } = params;

  return (
    <div className="min-h-screen bg-slate-50 flex flex-col overflow-hidden">
      <header className="shrink-0 border-b border-slate-200 bg-white px-5 py-3">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={() => window.close()} className="rounded-lg hover:bg-slate-100">
            <ChevronLeft className="h-5 w-5 text-slate-500" />
          </Button>
          <div className="flex items-center gap-3">
            <Boxes className="h-5 w-5 text-blue-600" />
            <div>
              <h1 className="text-base font-black uppercase tracking-tight text-slate-900 leading-none">
                Consolidar codigo
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
          <UnificarProdutosContent
            cnpj={cnpj}
            codigo={codigo}
            onSuccess={() => undefined}
            onCancel={() => window.close()}
            embedded={false}
          />
        </div>
      </main>
    </div>
  );
}
