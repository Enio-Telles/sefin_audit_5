import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Shield, Play, History, Loader2, Clock, ArrowLeft, CheckCircle2, AlertCircle, XCircle, Circle } from "lucide-react";
import { toast } from "sonner";
import {
  useAuditHistory,
  useAuditDetails,
  useRunAudit,
  useAuditPolling,
} from "@/hooks/useAuditoria";
import { AuditHistoryList } from "@/components/auditoria/AuditHistoryList";
import { AuditResultView } from "@/components/auditoria/AuditResultView";
import type {
  AuditPipelineFinalResult,
  AuditPipelineStatusResponse,
} from "@/lib/pythonApi";

export default function AuditarCNPJ() {
  const [cnpj, setCnpj] = useState("");
  const [dataLimiteProcessamento, setDataLimiteProcessamento] = useState("");

  // Tab & History State
  const [activeTab, setActiveTab] = useState("nova");
  const [viewingHistoryCnpj, setViewingHistoryCnpj] = useState<string | null>(
    null
  );

  // Execution state (New Audit)
  const [currentStep, setCurrentStep] = useState<string>("");
  const [elapsed, setElapsed] = useState<string>("");
  const [timerInterval, setTimerInterval] = useState<NodeJS.Timeout | null>(
    null
  );
  const [executionResult, setExecutionResult] =
    useState<AuditPipelineFinalResult | null>(null);
  const [activeAuditCnpj, setActiveAuditCnpj] = useState<string | null>(null);

  // React Query Hooks
  const { data: history = [], isLoading: isLoadingHistory } = useAuditHistory();
  const { data: historyDetail, isLoading: isLoadingDetail } =
    useAuditDetails(viewingHistoryCnpj);
  const runAuditMutation = useRunAudit();
  const pollingQuery = useAuditPolling(activeAuditCnpj);

  const formatCnpj = (value: string) => {
    const digits = value.replace(/\D/g, "").slice(0, 14);
    if (digits.length <= 2) return digits;
    if (digits.length <= 5) return `${digits.slice(0, 2)}.${digits.slice(2)}`;
    if (digits.length <= 8)
      return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5)}`;
    if (digits.length <= 12)
      return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8)}`;
    return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8, 12)}-${digits.slice(12)}`;
  };

  // Cleanup timer
  useEffect(() => {
    return () => {
      if (timerInterval) clearInterval(timerInterval);
    };
  }, [timerInterval]);

  // Handle polling results
  useEffect(() => {
    if (!activeAuditCnpj || !pollingQuery.data) return;

    const data = pollingQuery.data;
    const status = data.job_status;

    if (status === "executando") {
      setCurrentStep(data.message || "Executando auditoria...");
    } else if (status === "agendada") {
      setCurrentStep(data.message || "Auditoria agendada...");
    } else if (status === "concluida") {
      if (timerInterval) {
        clearInterval(timerInterval);
        setTimerInterval(null);
      }
      setCurrentStep("");
      setActiveAuditCnpj(null);
      setExecutionResult(data as AuditPipelineFinalResult);
      toast.success("Auditoria concluída!", {
        description: `${data.arquivos_extraidos?.length || 0} consultas + ${data.arquivos_analises?.length || 0} análises + ${data.arquivos_relatorios?.length || 0} relatórios`,
      });
    } else if (status === "erro") {
      if (timerInterval) {
        clearInterval(timerInterval);
        setTimerInterval(null);
      }
      setCurrentStep("");
      setActiveAuditCnpj(null);
      toast.error("Erro na auditoria", {
        description: data.message || "Falha no processamento em segundo plano.",
      });
    }
  }, [pollingQuery.data, activeAuditCnpj, timerInterval]);

  const handleAudit = () => {
    const cnpjLimpo = cnpj.replace(/\D/g, "");
    if (cnpjLimpo.length !== 14) {
      toast.error("Informe um CNPJ válido com 14 dígitos");
      return;
    }

    setExecutionResult(null);
    setCurrentStep("Conectando ao Oracle e extraindo dados...");

    const start = Date.now();
    const interval = setInterval(() => {
      const diff = Math.floor((Date.now() - start) / 1000);
      const min = Math.floor(diff / 60);
      const sec = diff % 60;
      setElapsed(min > 0 ? `${min}m ${sec}s` : `${sec}s`);
    }, 1000);
    setTimerInterval(interval);

    runAuditMutation.mutate(
      { cnpj: cnpjLimpo, dataLimite: dataLimiteProcessamento },
      {
        onSuccess: data => {
          // Do not stop timer or show success toast here.
          // Just set the active CNPJ to start polling.
          setActiveAuditCnpj(cnpjLimpo);
          setCurrentStep("Auditoria agendada. Aguardando processamento...");
        },
        onError: (err: any) => {
          clearInterval(interval);
          setTimerInterval(null);
          setCurrentStep("");
          setActiveAuditCnpj(null);
          toast.error("Erro ao agendar auditoria", {
            description: err.message,
          });
        },
      }
    );
  };

  const handleBackToHistory = () => {
    setViewingHistoryCnpj(null);
  };

  return (
    <div className="space-y-6">
      <div className="space-y-1">
        <h1 className="text-2xl font-bold tracking-tight flex items-center gap-2">
          <Shield className="h-6 w-6 text-primary" />
          Auditar CNPJ
        </h1>
        <p className="text-sm text-muted-foreground">
          Pipeline completo: Extração Oracle → Parquets → Análises → Relatórios
        </p>
      </div>

      <Tabs
        value={activeTab}
        onValueChange={val => {
          setActiveTab(val);
          if (val === "nova") {
            setViewingHistoryCnpj(null);
          }
        }}
      >
        <TabsList className="mb-4">
          <TabsTrigger value="nova" className="flex items-center gap-2">
            <Play className="h-4 w-4" />
            Nova Auditoria
          </TabsTrigger>
          <TabsTrigger value="historico" className="flex items-center gap-2">
            <History className="h-4 w-4" />
            Histórico
          </TabsTrigger>
        </TabsList>

        <TabsContent value="nova">
          <Card>
            <CardContent className="pt-6">
              <div className="flex items-end gap-4">
                <div className="space-y-2 flex-1 max-w-md">
                  <Label>CNPJ do Contribuinte</Label>
                  <Input
                    placeholder="00.000.000/0000-00"
                    value={cnpj}
                    onChange={e => setCnpj(formatCnpj(e.target.value))}
                    className="font-mono text-lg h-12"
                    disabled={runAuditMutation.isPending}
                  />
                </div>
                <div className="space-y-2 flex-1 max-w-xs">
                  <Label>Data Limite de Processamento da EFD</Label>
                  <Input
                    placeholder="DD/MM/AAAA (opcional)"
                    value={dataLimiteProcessamento}
                    onChange={e => setDataLimiteProcessamento(e.target.value)}
                    className="font-mono h-12"
                    disabled={runAuditMutation.isPending}
                  />
                </div>
                <Button
                  size="lg"
                  className="h-12 px-8 gap-2"
                  onClick={handleAudit}
                  disabled={
                    runAuditMutation.isPending ||
                    !!activeAuditCnpj ||
                    cnpj.replace(/\D/g, "").length !== 14
                  }
                >
                  {runAuditMutation.isPending || !!activeAuditCnpj ? (
                    <Loader2 className="h-5 w-5 animate-spin" />
                  ) : (
                    <Play className="h-5 w-5" />
                  )}
                  {runAuditMutation.isPending || !!activeAuditCnpj
                    ? "Processando..."
                    : "Iniciar Auditoria"}
                </Button>
              </div>

              {(runAuditMutation.isPending || !!activeAuditCnpj) && (
                <div className="mt-4 space-y-4 rounded-lg bg-primary/5 border border-primary/20 p-4 animate-in fade-in">
                  <div className="flex items-center gap-3">
                    <Loader2 className="h-5 w-5 animate-spin text-primary" />
                    <div>
                      <p className="text-sm font-medium">{currentStep}</p>
                      <p className="text-xs text-muted-foreground flex items-center gap-1 mt-0.5">
                        <Clock className="h-3 w-3" /> {elapsed}
                      </p>
                    </div>
                  </div>

                  {pollingQuery.data?.etapas && pollingQuery.data.etapas.length > 0 && (
                    <div className="mt-4 space-y-2 border-t border-primary/10 pt-4">
                      <p className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-2">Progresso do Pipeline</p>
                      {pollingQuery.data.etapas.map((etapa, idx) => (
                        <div key={idx} className="flex items-center gap-2 text-sm">
                          {etapa.status === "concluida" ? (
                            <CheckCircle2 className="h-4 w-4 text-emerald-500" />
                          ) : etapa.status === "executando" ? (
                            <Loader2 className="h-4 w-4 text-primary animate-spin" />
                          ) : etapa.status === "erro" ? (
                            <XCircle className="h-4 w-4 text-red-500" />
                          ) : (
                            <Circle className="h-4 w-4 text-muted-foreground/30" />
                          )}
                          <span className={etapa.status === "pendente" ? "text-muted-foreground" : "font-medium"}>
                            {etapa.etapa}
                          </span>
                        </div>
                      ))}
                    </div>
                  )}

                  {pollingQuery.data?.erros && pollingQuery.data.erros.length > 0 && (
                    <div className="mt-4 border-t border-primary/10 pt-4">
                      <div className="flex items-center gap-2 text-red-500 mb-2">
                        <AlertCircle className="h-4 w-4" />
                        <span className="text-xs font-semibold uppercase tracking-wider">Erros Registrados</span>
                      </div>
                      <div className="space-y-1">
                        {pollingQuery.data.erros.map((erro, idx) => (
                          <p key={idx} className="text-xs font-mono text-red-400 bg-red-500/10 p-1.5 rounded">
                            {erro}
                          </p>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              )}
            </CardContent>
          </Card>

          {executionResult &&
            !runAuditMutation.isPending &&
            !activeAuditCnpj && (
              <div className="mt-6">
                <AuditResultView result={executionResult} elapsed={elapsed} />
              </div>
            )}
        </TabsContent>

        <TabsContent value="historico">
          {!viewingHistoryCnpj ? (
            <AuditHistoryList
              history={history}
              loading={isLoadingHistory}
              onViewHistory={setViewingHistoryCnpj}
            />
          ) : (
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <Button
                  variant="ghost"
                  onClick={handleBackToHistory}
                  className="gap-2"
                >
                  <ArrowLeft className="h-4 w-4" />
                  Voltar para Lista
                </Button>
                <Badge variant="outline" className="text-sm py-1 font-mono">
                  Histórico: {formatCnpj(viewingHistoryCnpj)}
                </Badge>
              </div>

              {isLoadingDetail ? (
                <div className="flex items-center gap-3 p-6 border rounded-lg bg-card justify-center">
                  <Loader2 className="h-6 w-6 animate-spin text-primary" />
                  <span className="font-medium text-muted-foreground">
                    Carregando detalhes do histórico...
                  </span>
                </div>
              ) : historyDetail ? (
                <AuditResultView
                  result={historyDetail}
                  elapsed={"Visualização do Histórico"}
                />
              ) : (
                <div className="text-center py-8 text-muted-foreground border rounded-lg bg-card">
                  Detalhes não encontrados para este CNPJ.
                </div>
              )}
            </div>
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}
