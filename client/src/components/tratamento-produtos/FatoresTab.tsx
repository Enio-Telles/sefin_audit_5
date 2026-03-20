import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { KpiCard } from "./KpiCard";

export function FatoresTab({
  fatoresMissing,
  fatoresIssuesLength,
  fatoresCriticos,
  fatoresAltos,
  totalRegistros,
  hasFatoresFile,
  onOpenRevisaoFatores,
  onOpenFatoresParquet,
}: {
  fatoresMissing: boolean;
  fatoresIssuesLength: number;
  fatoresCriticos: number;
  fatoresAltos: number;
  totalRegistros: number;
  hasFatoresFile: boolean;
  onOpenRevisaoFatores: () => void;
  onOpenFatoresParquet: () => void;
}) {
  return (
    <Card className="border-border/70 bg-card/95 shadow-sm">
      <CardHeader className="pb-3">
        <CardTitle className="text-lg text-foreground">Fatores de conversao</CardTitle>
        <CardDescription className="text-muted-foreground">
          A revisao de fatores continua separada na operacao detalhada, mas agora nasce dentro da mesma jornada.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {fatoresMissing ? (
          <Alert className="border-amber-500/30 bg-amber-500/10">
            <AlertTitle>Fatores ainda nao calculados</AlertTitle>
            <AlertDescription>
              Rode o calculo quando quiser abrir a fila operacional de unidades e conversoes.
            </AlertDescription>
          </Alert>
        ) : null}

        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <KpiCard label="Ocorrencias" value={fatoresIssuesLength} helper="Total de alertas operacionais encontrados." accent />
          <KpiCard label="Criticos" value={fatoresCriticos} helper="Fatores invalidos ou extremos com maior risco." />
          <KpiCard label="Altos" value={fatoresAltos} helper="Casos relevantes, mas abaixo do nivel critico." />
          <KpiCard label="Registros" value={totalRegistros} helper="Total de linhas analisadas no parquet de fatores." />
        </div>

        <div className="flex flex-wrap gap-2">
          <Button className="gap-2 bg-blue-600 text-white hover:bg-blue-700" onClick={onOpenRevisaoFatores}>
            Abrir revisao de fatores
          </Button>
          <Button variant="outline" className="gap-2" onClick={onOpenFatoresParquet} disabled={!hasFatoresFile}>
            Abrir parquet de fatores
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
