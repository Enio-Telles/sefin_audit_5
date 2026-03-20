import { useLocation } from "wouter";
import { Activity, Boxes, ClipboardList, PlayCircle, Shield } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

function WorkspaceCard({
  icon: Icon,
  title,
  description,
  onClick,
  actionLabel,
}: {
  icon: typeof Shield;
  title: string;
  description: string;
  actionLabel: string;
  onClick: () => void;
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
      <CardContent>
        <Button className="gap-2" onClick={onClick}>
          <Activity className="h-4 w-4" />
          {actionLabel}
        </Button>
      </CardContent>
    </Card>
  );
}

export default function AuditoriaWorkspace() {
  const [, navigate] = useLocation();

  return (
    <div className="container mx-auto max-w-7xl space-y-6 py-6">
      <div className="space-y-3">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-3xl font-black tracking-tight text-foreground">Auditoria</h1>
          <Badge variant="outline" className="border-primary/30 bg-primary/10 text-primary">
            workspace
          </Badge>
        </div>
        <p className="max-w-3xl text-sm leading-6 text-muted-foreground">
          Nova camada de navegação para separar execução, casos auditados e tratamento de produtos sem desmontar o fluxo atual.
        </p>
      </div>

      <div className="grid gap-4 lg:grid-cols-3">
        <WorkspaceCard
          icon={PlayCircle}
          title="Execuções"
          description="Entrada operacional para iniciar uma auditoria e acompanhar o resultado mais recente."
          actionLabel="Abrir execuções"
          onClick={() => navigate("/auditoria/execucoes")}
        />
        <WorkspaceCard
          icon={ClipboardList}
          title="Casos"
          description="Lista de CNPJs auditados recentemente, com entrada para histórico e reabertura do caso."
          actionLabel="Abrir casos"
          onClick={() => navigate("/auditoria/casos")}
        />
        <WorkspaceCard
          icon={Boxes}
          title="Tratamento de produtos"
          description="Ponto único para entrar na revisão final, fatores e jornada de tratamento de produtos."
          actionLabel="Abrir tratamento"
          onClick={() => navigate("/tratamento-produtos")}
        />
      </div>

      <Card className="border-border/70 bg-card/95 shadow-sm">
        <CardHeader>
          <CardTitle>Compatibilidade preservada</CardTitle>
          <CardDescription>
            As rotas antigas continuam válidas. Esta página só organiza a navegação e prepara a transição para a estrutura nova.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-2">
          <Button variant="outline" onClick={() => navigate("/auditar")}>Abrir fluxo atual de auditoria</Button>
          <Button variant="outline" onClick={() => navigate("/analise-produtos")}>Abrir análise de produtos atual</Button>
        </CardContent>
      </Card>
    </div>
  );
}
