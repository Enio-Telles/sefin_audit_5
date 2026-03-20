import { useLocation } from "wouter";
import { Boxes, FileSpreadsheet, FolderTree, ShieldCheck } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

function HubCard({
  icon: Icon,
  title,
  description,
  primaryLabel,
  onPrimary,
  secondaryLabel,
  onSecondary,
}: {
  icon: typeof Boxes;
  title: string;
  description: string;
  primaryLabel: string;
  onPrimary: () => void;
  secondaryLabel?: string;
  onSecondary?: () => void;
}) {
  return (
    <Card className="border-border/70 bg-card/95 shadow-sm">
      <CardHeader className="pb-3">
        <div className="flex items-center gap-3">
          <div className="rounded-xl bg-primary/10 p-2">
            <Icon className="h-5 w-5 text-primary" />
          </div>
          <div>
            <CardTitle className="text-lg">{title}</CardTitle>
            <CardDescription>{description}</CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent className="flex flex-wrap gap-2">
        <Button onClick={onPrimary}>{primaryLabel}</Button>
        {secondaryLabel && onSecondary ? (
          <Button variant="outline" onClick={onSecondary}>{secondaryLabel}</Button>
        ) : null}
      </CardContent>
    </Card>
  );
}

export default function TratamentoProdutosHub() {
  const [, navigate] = useLocation();
  const params = new URLSearchParams(window.location.search);
  const cnpj = (params.get("cnpj") || "").replace(/\D/g, "");
  const suffix = cnpj ? `?cnpj=${cnpj}` : "";

  return (
    <div className="container mx-auto max-w-7xl space-y-6 py-6">
      <div className="space-y-2">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-3xl font-black tracking-tight text-foreground">Tratamento de produtos</h1>
          <Badge variant="outline" className="border-primary/30 bg-primary/10 text-primary">
            jornada
          </Badge>
          {cnpj ? <Badge variant="outline">{cnpj}</Badge> : null}
        </div>
        <p className="max-w-3xl text-sm leading-6 text-muted-foreground">
          Ponto de entrada para revisão final, fatores e camadas técnicas. Nesta fase, o hub organiza a navegação e reaproveita as páginas operacionais existentes.
        </p>
      </div>

      <div className="grid gap-4 lg:grid-cols-3">
        <HubCard
          icon={Boxes}
          title="Revisão final"
          description="Fila principal de decisão para consolidar ou separar grupos de produtos."
          primaryLabel="Abrir revisão"
          onPrimary={() => navigate(`/analise-produtos${suffix ? `${suffix}&tab=revisao` : "?tab=revisao"}`)}
          secondaryLabel="Resumo"
          onSecondary={() => navigate(`/analise-produtos${suffix ? `${suffix}&tab=resumo` : "?tab=resumo"}`)}
        />
        <HubCard
          icon={FileSpreadsheet}
          title="Fatores de conversão"
          description="Entrada para revisão operacional das unidades e fatores derivados."
          primaryLabel="Abrir fatores"
          onPrimary={() => navigate(`/analise-produtos${suffix ? `${suffix}&tab=fatores` : "?tab=fatores"}`)}
          secondaryLabel="Tela detalhada"
          onSecondary={() => navigate(`/revisao-fatores${suffix}`)}
        />
        <HubCard
          icon={FolderTree}
          title="Camada técnica"
          description="Atalhos para sugestões, artefatos e exploração das tabelas do runtime."
          primaryLabel="Abrir avançado"
          onPrimary={() => navigate(`/analise-produtos${suffix ? `${suffix}&tab=avancado` : "?tab=avancado"}`)}
          secondaryLabel="Sugestões"
          onSecondary={() => navigate(`/analise-produtos${suffix ? `${suffix}&tab=sugestoes` : "?tab=sugestoes"}`)}
        />
      </div>

      <Card className="border-border/70 bg-card/95 shadow-sm">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <ShieldCheck className="h-4 w-4 text-primary" />
            Observação de implementação
          </CardTitle>
          <CardDescription>
            O subsistema novo ainda reaproveita a tela `AnaliseProdutos` como motor operacional. O ganho imediato desta entrega é estrutural: navegação, agrupamento e preparação da próxima refatoração.
          </CardDescription>
        </CardHeader>
      </Card>
    </div>
  );
}
