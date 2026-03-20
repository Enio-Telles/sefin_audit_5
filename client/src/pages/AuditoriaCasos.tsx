import { useState } from "react";
import { useLocation } from "wouter";
import { History, Boxes } from "lucide-react";

import { AuditHistoryList } from "@/components/auditoria/AuditHistoryList";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { useAuditHistory } from "@/hooks/useAuditoria";

export default function AuditoriaCasos() {
  const [, navigate] = useLocation();
  const { data: history = [], isLoading } = useAuditHistory();
  const [selectedCnpj, setSelectedCnpj] = useState<string | null>(null);

  return (
    <div className="container mx-auto max-w-7xl space-y-6 py-6">
      <div className="space-y-2">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-3xl font-black tracking-tight text-foreground">Casos auditados</h1>
          <Badge variant="outline" className="border-primary/30 bg-primary/10 text-primary">
            histórico
          </Badge>
        </div>
        <p className="max-w-3xl text-sm leading-6 text-muted-foreground">
          Visão separada para navegar pelos contribuintes já auditados. O detalhamento profundo continua no fluxo atual.
        </p>
      </div>

      {selectedCnpj ? (
        <Card className="border-border/70 bg-card/95 shadow-sm">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <History className="h-4 w-4 text-primary" />
              Caso selecionado
            </CardTitle>
            <CardDescription>
              CNPJ selecionado: {selectedCnpj}. Nesta primeira implementação, o aprofundamento abre o fluxo atual já existente.
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-wrap gap-2">
            <Button onClick={() => navigate("/auditar")}>Abrir tela atual de auditoria</Button>
            <Button variant="outline" onClick={() => navigate(`/analise-produtos?cnpj=${selectedCnpj}`)}>
              <Boxes className="mr-2 h-4 w-4" />
              Abrir tratamento de produtos
            </Button>
            <Button variant="ghost" onClick={() => setSelectedCnpj(null)}>Voltar para lista</Button>
          </CardContent>
        </Card>
      ) : null}

      <AuditHistoryList history={history} loading={isLoading} onViewHistory={setSelectedCnpj} />
    </div>
  );
}
