import { useState, useEffect } from "react";
import { useLocation } from "wouter";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Checkbox } from "@/components/ui/checkbox";
import { Card, CardContent, CardDescription, CardHeader, CardTitle, CardFooter } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { ArrowLeft, Play, Loader2, CheckCircle2, XCircle, Activity } from "lucide-react";
import { toast } from "sonner";
import { useMutation, useQuery } from "@tanstack/react-query";
import { runBatchAudit, getAvailableQueries, LoteCNPJRequest } from "@/lib/pythonApi";
import { Progress } from "@/components/ui/progress";
import { useSSE } from "@/hooks/useSSE";

export default function LoteAuditoria() {
    const [, setLocation] = useLocation();

    const [cnpjsInput, setCnpjsInput] = useState("");
    const [selectedQueries, setSelectedQueries] = useState<string[]>([]);
    const [gerarExcel, setGerarExcel] = useState(true);
    const [gerarFisconforme, setGerarFisconforme] = useState(true);

    const [nomeAuditor, setNomeAuditor] = useState("");
    const [matriculaAuditor, setMatriculaAuditor] = useState("");
    const [emailAuditor, setEmailAuditor] = useState("");
    const [orgao, setOrgao] = useState("GERÊNCIA DE FISCALIZAÇÃO");
    const [numeroDSF, setNumeroDSF] = useState("");
    const [dataLimite, setDataLimite] = useState("");

    const { data: queriesData, isLoading: isLoadingQueries } = useQuery({
        queryKey: ["available-queries"],
        queryFn: getAvailableQueries,
    });

    const { connect, currentProgress, lastMessage, setStatus, status, setEvents, setCurrentProgress } = useSSE();

    const batchMutation = useMutation({
        mutationFn: (data: LoteCNPJRequest) => runBatchAudit(data),
        onSuccess: (data) => {
            // No backend assíncrono, o sucesso inicial significa que o background task começou
            connect(); // Inicia escuta SSE
            toast.info("Processamento iniciado em segundo plano", {
                description: `Acompanhe o progresso na tela.`,
            });
        },
        onError: (error) => {
            toast.error("Erro ao iniciar processamento", {
                description: error instanceof Error ? error.message : "Erro desconhecido",
            });
        },
    });

    const handleRunBatch = () => {
        // Extrai CNPJs quebrando por linha, vírgula ou ponto e vírgula
        const rawCnpjs = cnpjsInput.split(/[\n,;]+/).map(s => s.trim()).filter(Boolean);

        if (rawCnpjs.length === 0) {
            toast.error("Aviso", {
                description: "Insira pelo menos um CNPJ para processar.",
            });
            return;
        }

        if (selectedQueries.length === 0 && !gerarFisconforme) {
            toast.error("Aviso", {
                description: "Selecione pelo menos uma consulta ou ative o relatório Fisconforme.",
            });
            return;
        }

        if (gerarFisconforme && (!nomeAuditor || !matriculaAuditor)) {
            toast.error("Aviso", {
                description: "Nome e Matrícula do Auditor são obrigatórios para o Fisconforme.",
            });
            return;
        }

        batchMutation.mutate({
            cnpjs: rawCnpjs,
            queries: selectedQueries,
            gerar_excel: gerarExcel,
            gerar_relatorio_fisconforme: gerarFisconforme,
            nome_auditor: nomeAuditor,
            matricula_auditor: matriculaAuditor,
            email_auditor: emailAuditor,
            orgao: orgao,
            numero_DSF: numeroDSF,
            data_limite_processamento: dataLimite || undefined
        });
    };

    const toggleQuery = (queryId: string) => {
        setSelectedQueries(prev =>
            prev.includes(queryId)
                ? prev.filter(q => q !== queryId)
                : [...prev, queryId]
        );
    };

    return (
        <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
            <div className="flex items-center space-x-4 mb-4">
                <Button variant="ghost" size="icon" onClick={() => setLocation("/")} aria-label="Voltar" title="Voltar">
                    <ArrowLeft className="h-5 w-5" />
                </Button>
                <h1 className="text-3xl font-bold tracking-tight text-slate-900 border-l-4 border-indigo-500 pl-4 h-8 flex items-center">
                    Processamento em Lote
                </h1>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                {/* Lado Esquerdo: Inputs Principais */}
                <div className="space-y-6">
                    <Card>
                        <CardHeader>
                            <CardTitle>Lista de CNPJs</CardTitle>
                            <CardDescription>
                                Cole a lista de CNPJs (um por linha, ou separados por vírgula).
                            </CardDescription>
                        </CardHeader>
                        <CardContent>
                            <Textarea
                                placeholder="Exemplo:&#10;00.000.000/0001-91&#10;11.111.111/0001-11"
                                className="min-h-[200px]"
                                value={cnpjsInput}
                                onChange={(e) => setCnpjsInput(e.target.value)}
                            />
                        </CardContent>
                    </Card>

                    <Card>
                        <CardHeader>
                            <CardTitle>Configurações Gerais</CardTitle>
                        </CardHeader>
                        <CardContent className="space-y-4">
                            <div className="flex items-center space-x-2">
                                <Checkbox
                                    id="excel"
                                    checked={gerarExcel}
                                    onCheckedChange={(c) => setGerarExcel(c === true)}
                                />
                                <Label htmlFor="excel">Gerar arquivo Excel com todas as consultas</Label>
                            </div>
                            <div className="space-y-2">
                                <Label htmlFor="data_limite">Data Limite de Processamento (Opcional)</Label>
                                <Input
                                    id="data_limite"
                                    type="date"
                                    value={dataLimite}
                                    onChange={(e) => setDataLimite(e.target.value)}
                                    className="max-w-[200px]"
                                />
                            </div>
                        </CardContent>
                    </Card>

                    <Card>
                        <CardHeader>
                            <CardTitle>Relatório Fisconforme</CardTitle>
                            <CardDescription>
                                Informações para preenchimento do termo de notificação.
                            </CardDescription>
                        </CardHeader>
                        <CardContent className="space-y-4">
                            <div className="flex items-center space-x-2 pb-2 mb-2 border-b">
                                <Checkbox
                                    id="fisconforme"
                                    checked={gerarFisconforme}
                                    onCheckedChange={(c) => setGerarFisconforme(c === true)}
                                />
                                <Label htmlFor="fisconforme" className="font-semibold text-primary">Gerar Relatório Automático</Label>
                            </div>

                            {gerarFisconforme && (
                                <div className="space-y-4 animate-in fade-in duration-300">
                                    <div className="grid grid-cols-2 gap-4">
                                        <div className="space-y-2">
                                            <Label htmlFor="nome_auditor">Nome do Auditor(a)*</Label>
                                            <Input
                                                id="nome_auditor"
                                                value={nomeAuditor}
                                                onChange={(e) => setNomeAuditor(e.target.value)}
                                                placeholder="João Silva"
                                            />
                                        </div>
                                        <div className="space-y-2">
                                            <Label htmlFor="matricula">Matrícula*</Label>
                                            <Input
                                                id="matricula"
                                                value={matriculaAuditor}
                                                onChange={(e) => setMatriculaAuditor(e.target.value)}
                                                placeholder="123456-7"
                                            />
                                        </div>
                                    </div>
                                    <div className="grid grid-cols-2 gap-4">
                                        <div className="space-y-2">
                                            <Label htmlFor="orgao">Órgão</Label>
                                            <Input
                                                id="orgao"
                                                value={orgao}
                                                onChange={(e) => setOrgao(e.target.value)}
                                            />
                                        </div>
                                        <div className="space-y-2">
                                            <Label htmlFor="email">E-mail para Contato (Opcional)</Label>
                                            <Input
                                                id="email"
                                                type="email"
                                                value={emailAuditor}
                                                onChange={(e) => setEmailAuditor(e.target.value)}
                                                placeholder="auditor@sefin.ro.gov.br"
                                            />
                                        </div>
                                    </div>
                                    <div className="grid grid-cols-2 gap-4">
                                        <div className="space-y-2">
                                            <Label htmlFor="numero_dsf">Número DSF</Label>
                                            <Input
                                                id="numero_dsf"
                                                value={numeroDSF}
                                                onChange={(e) => setNumeroDSF(e.target.value)}
                                                placeholder="20263710400226"
                                            />
                                        </div>
                                    </div>
                                </div>
                            )}
                        </CardContent>
                    </Card>
                </div>

                {/* Lado Direito: Seleção de Consultas e Logs */}
                <div className="space-y-6">
                    <Card>
                        <CardHeader>
                            <CardTitle>Consultas Base</CardTitle>
                            <CardDescription>
                                Selecione quais SQLs extrair para cada CNPJ.
                            </CardDescription>
                        </CardHeader>
                        <CardContent>
                            {isLoadingQueries ? (
                                <div className="flex items-center space-x-2 text-muted-foreground p-4">
                                    <Loader2 className="h-4 w-4 animate-spin" />
                                    <span>Carregando consultas...</span>
                                </div>
                            ) : queriesData?.consultas && queriesData.consultas.length > 0 ? (
                                <div className="grid grid-cols-1 gap-2 max-h-[300px] overflow-y-auto pr-2">
                                    {queriesData.consultas.map((q) => (
                                        <div key={q.id} className="flex items-center space-x-2 p-2 rounded-md hover:bg-slate-50 border">
                                            <Checkbox
                                                id={`query-${q.id}`}
                                                checked={selectedQueries.includes(q.id)}
                                                onCheckedChange={() => toggleQuery(q.id)}
                                            />
                                            <Label
                                                htmlFor={`query-${q.id}`}
                                                className="flex-1 cursor-pointer font-medium"
                                            >
                                                {q.nome}
                                            </Label>
                                        </div>
                                    ))}
                                </div>
                            ) : (
                                <div className="p-4 bg-amber-50 text-amber-800 rounded-md text-sm border border-amber-200">
                                    Nenhuma consulta encontrada na pasta `consultas_fonte`.
                                </div>
                            )}
                        </CardContent>
                        <CardFooter>
                            <div className="flex w-full justify-between items-center border-t pt-4">
                                <span className="text-sm font-medium text-slate-500">
                                    {selectedQueries.length} consultas selecionadas
                                </span>
                                <Button
                                    onClick={() => {
                                        const allIds = queriesData?.consultas?.map(q => q.id) || [];
                                        if (allIds.length > 0 && selectedQueries.length === allIds.length) {
                                            setSelectedQueries([]);
                                        } else {
                                            setSelectedQueries(allIds);
                                        }
                                    }}
                                    variant="outline"
                                    size="sm"
                                >
                                    {queriesData?.consultas && queriesData.consultas.length > 0 && selectedQueries.length === queriesData.consultas.length
                                        ? "Desmarcar Todas"
                                        : "Selecionar Todas"}
                                </Button>
                            </div>
                        </CardFooter>
                    </Card>

                    <Button
                        className="w-full h-14 text-lg font-bold shadow-md hover:shadow-lg transition-all"
                        size="lg"
                        onClick={handleRunBatch}
                        disabled={batchMutation.isPending || status === 'connected'}
                    >
                        {batchMutation.isPending || status === 'connected' ? (
                            <>
                                <Loader2 className="mr-2 h-6 w-6 animate-spin" />
                                {status === 'connected' ? 'Processando Lote...' : 'Iniciando...'}
                            </>
                        ) : (
                            <>
                                <Play className="mr-2 h-6 w-6" />
                                Executar Processamento em Lote
                            </>
                        )}
                    </Button>

                    {/* Barra de Progresso em Tempo Real (SSE) */}
                    {(status === 'connected' || status === 'connecting' || (status === 'idle' && currentProgress > 0)) && (
                        <Card className="border-indigo-100 bg-indigo-50/30 overflow-hidden shadow-sm animate-in fade-in slide-in-from-top-2">
                            <CardHeader className="py-3 px-4">
                                <div className="flex items-center justify-between">
                                    <CardTitle className="text-sm font-semibold flex items-center text-indigo-700">
                                        <Activity className="w-4 h-4 mr-2 animate-pulse" />
                                        {status === 'connected' ? 'Progresso do Lote' : 'Aguardando servidor...'}
                                    </CardTitle>
                                    <span className="text-xs font-mono font-bold text-indigo-600 bg-indigo-100 px-1.5 py-0.5 rounded">
                                        {Math.round(currentProgress)}%
                                    </span>
                                </div>
                            </CardHeader>
                            <CardContent className="py-0 px-4 pb-4">
                                <Progress value={currentProgress} className="h-2 bg-indigo-100" indicatorClassName="bg-indigo-600" />
                                <p className="text-[11px] text-slate-600 mt-2 italic truncate">
                                    {lastMessage || "Preparando ambiente..."}
                                </p>
                            </CardContent>
                        </Card>
                    )}

                    {/* Resultados */}
                    {batchMutation.isSuccess && batchMutation.data && (
                        <Card className="border-green-200 bg-green-50 shadow-sm animate-in zoom-in-95 duration-300">
                            <CardHeader className="pb-2">
                                <CardTitle className="text-green-800 flex items-center">
                                    <CheckCircle2 className="w-5 h-5 mr-2 text-green-600" />
                                    Processamento Finalizado
                                </CardTitle>
                            </CardHeader>
                            <CardContent>
                                <p className="text-sm text-green-700 mb-4 font-medium">
                                    {batchMutation.data.total_processados} CNPJs processados.
                                </p>
                                <div className="space-y-3 max-h-[300px] overflow-y-auto">
                                    {batchMutation.data.lote.map((res, i) => (
                                        <div key={i} className={`p-3 rounded border bg-white shadow-sm ${res.sucesso ? 'border-green-200' : 'border-red-200'}`}>
                                            <div className="flex items-center justify-between font-bold mb-1">
                                                <span>{res.cnpj}</span>
                                                {res.sucesso ? (
                                                    <span className="text-green-600 text-xs flex"><CheckCircle2 className="w-4 h-4 mr-1" /> OK</span>
                                                ) : (
                                                    <span className="text-red-500 text-xs flex"><XCircle className="w-4 h-4 mr-1" /> Falha</span>
                                                )}
                                            </div>
                                            {res.arquivos.length > 0 && (
                                                <div className="text-xs text-slate-600 mt-2">
                                                    <strong>Salvos:</strong> {res.arquivos.join(", ")}
                                                </div>
                                            )}
                                            {res.erros.length > 0 && (
                                                <div className="text-xs text-red-600 mt-2 pt-2 border-t border-red-100">
                                                    <strong>Erros:</strong>
                                                    <ul className="list-disc pl-4 mt-1">
                                                        {res.erros.map((e, j) => <li key={j}>{e}</li>)}
                                                    </ul>
                                                </div>
                                            )}
                                        </div>
                                    ))}
                                </div>
                            </CardContent>
                        </Card>
                    )}

                </div>
            </div>
        </div>
    );
}
