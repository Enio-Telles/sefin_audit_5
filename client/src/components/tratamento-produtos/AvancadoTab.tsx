import { FolderOpen, GitBranch, Table2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { ArtefactButton } from "./ArtefactButton";

export function AvancadoTab({
  hasTabelaFinal,
  hasStatusAnalise,
  hasBaseDetalhes,
  hasMapaAgregados,
  hasMapaDesagregados,
  hasSuggestionFile,
  onOpenTabelaFinal,
  onOpenStatusAnalise,
  onOpenBaseDetalhes,
  onOpenMapaAgregados,
  onOpenMapaDesagregados,
  onOpenSuggestionFile,
  onOpenTabelas,
  onOpenTabelaFinalBruta,
  onOpenUltimoArquivoSugestoes,
}: {
  hasTabelaFinal: boolean;
  hasStatusAnalise: boolean;
  hasBaseDetalhes: boolean;
  hasMapaAgregados: boolean;
  hasMapaDesagregados: boolean;
  hasSuggestionFile: boolean;
  onOpenTabelaFinal: () => void;
  onOpenStatusAnalise: () => void;
  onOpenBaseDetalhes: () => void;
  onOpenMapaAgregados: () => void;
  onOpenMapaDesagregados: () => void;
  onOpenSuggestionFile: () => void;
  onOpenTabelas: () => void;
  onOpenTabelaFinalBruta: () => void;
  onOpenUltimoArquivoSugestoes: () => void;
}) {
  return (
    <Card className="border-border/70 bg-card/95 shadow-sm">
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-lg text-foreground">
          Camada tecnica
        </CardTitle>
        <CardDescription className="text-muted-foreground">
          Arquivos e atalhos de apoio continuam disponiveis, mas fora do caminho principal do analista.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid gap-3 md:grid-cols-2">
          <ArtefactButton
            available={hasTabelaFinal}
            label="Tabela final"
            helper="Arquivo principal da revisao final ja desagregada."
            onClick={onOpenTabelaFinal}
          />
          <ArtefactButton
            available={hasStatusAnalise}
            label="Status de analise"
            helper="Historico operacional de verificados, consolidados e separados."
            onClick={onOpenStatusAnalise}
          />
          <ArtefactButton
            available={hasBaseDetalhes}
            label="Base de detalhes"
            helper="Camada mais tecnica, usada para rastrear descricoes e campos brutos."
            onClick={onOpenBaseDetalhes}
          />
          <ArtefactButton
            available={hasMapaAgregados}
            label="Mapa de agregados"
            helper="Rastreamento das decisoes de agregacao que alimentam a tabela final."
            onClick={onOpenMapaAgregados}
          />
          <ArtefactButton
            available={hasMapaDesagregados}
            label="Mapa de desagregados"
            helper="Rastreamento das separacoes de codigo aplicadas antes da tabela final."
            onClick={onOpenMapaDesagregados}
          />
          <ArtefactButton
            available={hasSuggestionFile}
            label="Arquivo de sugestoes"
            helper="Parquet bruto das sugestoes ativas para auditoria tecnica."
            onClick={onOpenSuggestionFile}
          />
        </div>

        <Separator />

        <div className="flex flex-wrap gap-2">
          <Button variant="outline" className="gap-2" onClick={onOpenTabelas}>
            <Table2 className="h-4 w-4" />
            Visualizar tabelas
          </Button>
          <Button variant="outline" className="gap-2" onClick={onOpenTabelaFinalBruta}>
            <FolderOpen className="h-4 w-4" />
            Abrir tabela final bruta
          </Button>
          <Button variant="outline" className="gap-2" onClick={onOpenUltimoArquivoSugestoes}>
            <GitBranch className="h-4 w-4" />
            Abrir ultimo arquivo de sugestoes
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
