import re

with open('client/src/pages/AuditarCNPJ.tsx', 'r') as f:
    content = f.read()

# 1. Add `useAuditPolling` import
content = content.replace(
    'import { useAuditHistory, useAuditDetails, useRunAudit } from "@/hooks/useAuditoria";',
    'import { useAuditHistory, useAuditDetails, useRunAudit, useAuditPolling } from "@/hooks/useAuditoria";'
)

# 2. Add state and polling hook
target_state = """    // Execution state (New Audit)
    const [currentStep, setCurrentStep] = useState<string>("");
    const [elapsed, setElapsed] = useState<string>("");
    const [timerInterval, setTimerInterval] = useState<NodeJS.Timeout | null>(null);
    const [executionResult, setExecutionResult] = useState<AuditPipelineResponse | null>(null);

    // React Query Hooks
    const { data: history = [], isLoading: isLoadingHistory } = useAuditHistory();
    const { data: historyDetail, isLoading: isLoadingDetail } = useAuditDetails(viewingHistoryCnpj);
    const runAuditMutation = useRunAudit();"""

replacement_state = """    // Execution state (New Audit)
    const [currentStep, setCurrentStep] = useState<string>("");
    const [elapsed, setElapsed] = useState<string>("");
    const [timerInterval, setTimerInterval] = useState<NodeJS.Timeout | null>(null);
    const [executionResult, setExecutionResult] = useState<AuditPipelineResponse | null>(null);
    const [activeAuditCnpj, setActiveAuditCnpj] = useState<string | null>(null);

    // React Query Hooks
    const { data: history = [], isLoading: isLoadingHistory } = useAuditHistory();
    const { data: historyDetail, isLoading: isLoadingDetail } = useAuditDetails(viewingHistoryCnpj);
    const runAuditMutation = useRunAudit();
    const pollingQuery = useAuditPolling(activeAuditCnpj);"""

content = content.replace(target_state, replacement_state)

# 3. Add polling useEffect
target_cleanup = """    // Cleanup timer
    useEffect(() => {
        return () => {
            if (timerInterval) clearInterval(timerInterval);
        };
    }, [timerInterval]);"""

replacement_cleanup = """    // Cleanup timer
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
            setExecutionResult(data);
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
            toast.error("Erro na auditoria", { description: data.message || "Falha no processamento em segundo plano." });
        }
    }, [pollingQuery.data, activeAuditCnpj, timerInterval]);"""

content = content.replace(target_cleanup, replacement_cleanup)

# 4. Modify handleAudit
target_handle_audit = """        runAuditMutation.mutate(
            { cnpj: cnpjLimpo, dataLimite: dataLimiteProcessamento },
            {
                onSuccess: (data) => {
                    clearInterval(interval);
                    setTimerInterval(null);
                    const finalDiff = Math.floor((Date.now() - start) / 1000);
                    const finalMin = Math.floor(finalDiff / 60);
                    const finalSec = finalDiff % 60;
                    setElapsed(finalMin > 0 ? `${finalMin}m ${finalSec}s` : `${finalSec}s`);
                    setCurrentStep("");

                    toast.success("Auditoria completa!", {
                        description: `${data.arquivos_extraidos.length} consultas + ${data.arquivos_analises.length} análises + ${data.arquivos_relatorios.length} relatórios`,
                    });
                    setExecutionResult(data);
                },
                onError: (err: any) => {
                    clearInterval(interval);
                    setTimerInterval(null);
                    setCurrentStep("");
                    toast.error("Erro na auditoria", { description: err.message });
                }
            }
        );"""

replacement_handle_audit = """        runAuditMutation.mutate(
            { cnpj: cnpjLimpo, dataLimite: dataLimiteProcessamento },
            {
                onSuccess: (data) => {
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
                    toast.error("Erro ao agendar auditoria", { description: err.message });
                }
            }
        );"""

content = content.replace(target_handle_audit, replacement_handle_audit)

# 5. Fix disabled state
target_disabled = """disabled={runAuditMutation.isPending || cnpj.replace(/\D/g, "").length !== 14}"""
replacement_disabled = """disabled={runAuditMutation.isPending || !!activeAuditCnpj || cnpj.replace(/\D/g, "").length !== 14}"""
content = content.replace(target_disabled, replacement_disabled)

# 6. Fix loading state
target_loading = """{runAuditMutation.isPending ? "Processando..." : "Iniciar Auditoria"}"""
replacement_loading = """{runAuditMutation.isPending || !!activeAuditCnpj ? "Processando..." : "Iniciar Auditoria"}"""
content = content.replace(target_loading, replacement_loading)

# 7. Fix loading icon
target_icon = """{runAuditMutation.isPending ? <Loader2 className="h-5 w-5 animate-spin" /> : <Play className="h-5 w-5" />}"""
replacement_icon = """{runAuditMutation.isPending || !!activeAuditCnpj ? <Loader2 className="h-5 w-5 animate-spin" /> : <Play className="h-5 w-5" />}"""
content = content.replace(target_icon, replacement_icon)

# 8. Fix loading container display
target_container = """{runAuditMutation.isPending && ("""
replacement_container = """{(runAuditMutation.isPending || !!activeAuditCnpj) && ("""
content = content.replace(target_container, replacement_container)

# 9. Fix execution result display
target_result = """{executionResult && !runAuditMutation.isPending && ("""
replacement_result = """{executionResult && !runAuditMutation.isPending && !activeAuditCnpj && ("""
content = content.replace(target_result, replacement_result)

with open('client/src/pages/AuditarCNPJ.tsx', 'w') as f:
    f.write(content)
