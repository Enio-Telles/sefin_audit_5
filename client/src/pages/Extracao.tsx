import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Progress } from "@/components/ui/progress";
import { Switch } from "@/components/ui/switch";
import { Checkbox } from "@/components/ui/checkbox";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogDescription } from "@/components/ui/dialog";
import {
  Database,
  Play,
  FolderOpen,
  CheckCircle2,
  XCircle,
  Loader2,
  FileCode,
  AlertCircle,
  RefreshCw,
} from "lucide-react";
import { useState, useCallback, useRef, useEffect } from "react";
import { toast } from "sonner";
import {
  testOracleConnection,
  extractOracleData,
  getOracleCredentials,
  saveOracleCredentials,
  clearOracleCredentials,
  listSqlQueries,
  listAuxiliaryQueries,
  getProjectPaths,
  type OracleConnectionConfig,
  type SqlQueryDefinition,
} from "@/lib/pythonApi";
import { FolderBrowserDialog } from "@/components/ui/folder-browser-dialog";


type ConnectionDiagnostic = {
  title: string;
  summary: string;
  hints: string[];
  tone: "amber" | "red" | "blue";
};

function buildOracleDiagnostic(message: string, host: string, port: number): ConnectionDiagnostic | null {
  const normalized = String(message || "").toLowerCase();
  if (!normalized) return null;

  if (normalized.includes("nao resolvido por dns") || normalized.includes("getaddrinfo failed")) {
    return {
      title: "Host Oracle nao resolvido",
      summary: `O nome ${host} nao foi resolvido por DNS antes da tentativa de conexao.`,
      hints: [
        "Verifique se o hostname Oracle esta correto.",
        "Confirme se a VPN/rede corporativa esta ativa.",
        `Teste externamente: nslookup ${host}`,
      ],
      tone: "red",
    };
  }

  if (normalized.includes("porta") && (normalized.includes("nao respondeu") || normalized.includes("nao esta acessivel"))) {
    return {
      title: "Porta Oracle inacessivel",
      summary: `O host foi resolvido, mas a porta ${port} nao respondeu como esperado.`,
      hints: [
        "Verifique firewall, VPN e disponibilidade do listener Oracle.",
        `Teste externamente: Test-NetConnection ${host} -Port ${port}`,
        "Confirme se a porta e o servico Oracle continuam corretos.",
      ],
      tone: "amber",
    };
  }

  if (normalized.includes("ora-01017")) {
    return {
      title: "Credenciais Oracle invalidas",
      summary: "O servidor respondeu, mas rejeitou usuario ou senha.",
      hints: [
        "Confira CPF/usuario e senha.",
        "Se usar credencial salva, remova e informe novamente.",
      ],
      tone: "red",
    };
  }

  if (normalized.includes("ora-12514") || normalized.includes("ora-12505")) {
    return {
      title: "Servico Oracle nao reconhecido",
      summary: "O host respondeu, mas o servico informado nao foi aceito pelo listener.",
      hints: [
        "Confira o campo Servico.",
        "Valide se o ambiente usa service_name ou outro alias.",
      ],
      tone: "amber",
    };
  }

  if (normalized.includes("driver oracle") || normalized.includes("oracledb")) {
    return {
      title: "Driver Oracle indisponivel",
      summary: "O backend nao conseguiu usar o driver Python do Oracle.",
      hints: ["Verifique a instalacao do pacote oracledb no ambiente Python."],
      tone: "blue",
    };
  }

  return {
    title: "Falha de conexao",
    summary: message,
    hints: [
      "Revise host, porta, servico, usuario e senha.",
      "Se o host for interno, valide VPN e DNS corporativo.",
    ],
    tone: "amber",
  };
}

export default function Extracao() {
  // Removido fallback com caminhos absolutos. Use diretórios do projeto via API ou escolha manual do usuário.
  const FALLBACK_SQL_DIR = "";
  const FALLBACK_OUTPUT_DIR = "";
  const [cnpj, setCnpj] = useState("");
  const [dataLimiteProcessamento, setDataLimiteProcessamento] = useState("");
  const [outputDir, setOutputDir] = useState(localStorage.getItem("sefin-audit-out-dir") || FALLBACK_OUTPUT_DIR);
  const [sqlDir, setSqlDir] = useState(localStorage.getItem("sefin-audit-sql-dir") || FALLBACK_SQL_DIR);

  const [isConnected, setIsConnected] = useState(false);
  const [isExtracting, setIsExtracting] = useState(false);
  const [isTesting, setIsTesting] = useState(false);
  const [progress, setProgress] = useState(0);

  const [availableQueries, setAvailableQueries] = useState<SqlQueryDefinition[]>([]);
  const [selectedQueries, setSelectedQueries] = useState<string[]>([]);
  const [auxiliaryQueries, setAuxiliaryQueries] = useState<SqlQueryDefinition[]>([]);

  const [includeAuxiliary, setIncludeAuxiliary] = useState(true);
  const [rememberCredentials, setRememberCredentials] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);
  const [connectionDiagnostic, setConnectionDiagnostic] = useState<ConnectionDiagnostic | null>(null);

  // Browser state
  const [browserType, setBrowserType] = useState<"output" | "sql" | null>(null);

  // SQL Parameters state
  const [isParamsDialogOpen, setIsParamsDialogOpen] = useState(false);
  const [requiredParams, setRequiredParams] = useState<string[]>([]);
  const [sqlParameters, setSqlParameters] = useState<Record<string, string>>({});

  const [connection, setConnection] = useState<OracleConnectionConfig>({
    host: "exa01-scan.sefin.ro.gov.br",
    port: 1521,
    service: "sefindw",
    user: "",
    password: "",
  });

  // Load saved credentials on mount
  useEffect(() => {
    async function fetchCredentials() {
      try {
        const res = await getOracleCredentials();
        if (res.success && res.has_credentials && res.user) {
          setConnection(prev => ({
            ...prev,
            user: res.user!,
          }));
          setRememberCredentials(true);
          addLog("Usuário Oracle carregado do cofre. A senha será usada apenas no servidor.");
        }
      } catch (err) {
        console.error("Failed to load credentials", err);
      }
    }
    fetchCredentials();

    // Dynamic project path resolution
    async function resolveProjectPaths() {
      try {
        const paths = await getProjectPaths();
        // Only update if user hasn't set custom dirs via localStorage
        if (!localStorage.getItem("sefin-audit-sql-dir") && paths.consultas_fonte) {
          setSqlDir(paths.consultas_fonte);
        }
      } catch {
        // Fallback to hardcoded path, silently
      }
    }
    resolveProjectPaths();
  }, []);

  // Load SQL Queries whenever sqlDir changes
  useEffect(() => {
    async function fetchQueries() {
      if (!sqlDir) {
        setAvailableQueries([]);
        return;
      }
      try {
        const res = await listSqlQueries(sqlDir);
        setAvailableQueries(res.queries || []);
      } catch (err: any) {
        setAvailableQueries([]);
        if (err?.status === 403) {
          try {
            const paths = await getProjectPaths();
            if (paths.consultas_fonte && paths.consultas_fonte !== sqlDir) {
              setSqlDir(paths.consultas_fonte);
              toast.warning("Pasta SQL fora da área permitida. Diretório padrão restaurado.");
            }
          } catch {
            // no-op
          }
        }
      }
    }
    fetchQueries();
  }, [sqlDir]);

  // Load auxiliary queries whenever sqlDir or includeAuxiliary changes
  useEffect(() => {
    async function fetchAuxQueries() {
      if (!sqlDir || !includeAuxiliary) {
        setAuxiliaryQueries([]);
        return;
      }
      const auxDir = sqlDir.replace(/[\\/]$/, "") + "\\auxiliares";
      try {
        const res = await listAuxiliaryQueries(auxDir);
        setAuxiliaryQueries(res.queries || []);
      } catch (err: any) {
        setAuxiliaryQueries([]);
        if (err?.status === 403) {
          try {
            const paths = await getProjectPaths();
            if (paths.consultas_fonte && paths.consultas_fonte !== sqlDir) {
              setSqlDir(paths.consultas_fonte);
              toast.warning("Pasta de auxiliares fora da área permitida. Diretório padrão restaurado.");
            }
          } catch {
            // no-op
          }
        }
      }
    }
    fetchAuxQueries();
  }, [sqlDir, includeAuxiliary]);

  // Persist directories
  useEffect(() => {
    localStorage.setItem("sefin-audit-out-dir", outputDir);
  }, [outputDir]);

  useEffect(() => {
    localStorage.setItem("sefin-audit-sql-dir", sqlDir);
  }, [sqlDir]);

  const addLog = (msg: string) => {
    const ts = new Date().toLocaleTimeString("pt-BR");
    setLogs((prev) => [...prev, `[${ts}] ${msg}`]);
  };

  const formatCnpj = (value: string) => {
    const digits = value.replace(/\D/g, "").slice(0, 14);
    if (digits.length <= 2) return digits;
    if (digits.length <= 5) return `${digits.slice(0, 2)}.${digits.slice(2)}`;
    if (digits.length <= 8) return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5)}`;
    if (digits.length <= 12) return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8)}`;
    return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8, 12)}-${digits.slice(12)}`;
  };

  const handleCnpjChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setCnpj(formatCnpj(e.target.value));
  };

  const toggleQuery = (queryId: string) => {
    setSelectedQueries((prev) =>
      prev.includes(queryId) ? prev.filter((id) => id !== queryId) : [...prev, queryId]
    );
  };

  const selectAllQueries = () => {
    if (selectedQueries.length === availableQueries.length) {
      setSelectedQueries([]);
    } else {
      setSelectedQueries(availableQueries.map((q) => q.id));
    }
  };

  const handleTestConnection = useCallback(async () => {
    if (!connection.user || !connection.password) {
      toast.error("Informe usuário e senha");
      return;
    }
    setIsTesting(true);
    addLog("Testando conexão Oracle...");
    try {
      const result = await testOracleConnection(connection);
      if (result.success) {
        setIsConnected(true);
        addLog("Conexão estabelecida com sucesso!");
        toast.success("Conexão Oracle estabelecida!");

        // Handle credential saving
        if (rememberCredentials) {
          try {
            await saveOracleCredentials(connection);
            addLog("Credenciais salvas no cofre com segurança.");
          } catch (e) {
            console.error("Failed to save credentials", e);
          }
        } else {
          try {
            await clearOracleCredentials();
            addLog("Credenciais removidas do cofre seguro.");
          } catch (e) {
            console.error("Failed to clear credentials", e);
          }
        }
      } else {
        setIsConnected(false);
        addLog(`Falha na conexão: ${result.message}`);
        toast.error("Falha na conexão", { description: result.message });
      }
    } catch (err: any) {
      setIsConnected(false);
      setConnectionDiagnostic(buildOracleDiagnostic(err.message, connection.host, connection.port));
      addLog(`Erro: ${err.message}`);
      toast.error("Erro ao testar conexão", { description: err.message });
    } finally {
      setIsTesting(false);
    }
  }, [connection]);

  const executeExtraction = async (paramsToBind: Record<string, string> = {}) => {
    setIsParamsDialogOpen(false);
    setIsExtracting(true);
    setProgress(0);
    addLog(`Iniciando extração${cnpj ? ` para CNPJ: ${cnpj}` : ' (Geral sem CNPJ)'}`);
    if (dataLimiteProcessamento) addLog(`Data limite de processamento: ${dataLimiteProcessamento}`);
    addLog(`Consultas: ${selectedQueries.length} arquivos selecionados`);
    addLog(`Diretório de saída: ${outputDir}`);

    // Sempre inclui DATA_LIMITE_PROCESSAMENTO nos parâmetros
    const allParams: Record<string, string> = {
      ...paramsToBind,
      DATA_LIMITE_PROCESSAMENTO: dataLimiteProcessamento || "",
    };

    try {
      const total = selectedQueries.length + (includeAuxiliary ? 1 : 0);
      let completed = 0;

      const result = await extractOracleData({
        connection,
        cnpj: cnpj ? cnpj.replace(/\D/g, "") : "",
        output_dir: outputDir,
        queries: selectedQueries,
        include_auxiliary: includeAuxiliary,
        auxiliary_queries_dir: includeAuxiliary ? (sqlDir.replace(/[\\/]$/, "") + "\\auxiliares") : undefined,
        normalize_columns: true,
        parameters: allParams,
      });

      if (result.success) {
        setConnectionDiagnostic(null);
        for (const r of result.results) {
          completed++;
          setProgress(Math.round((completed / total) * 100));
          if (r.status === "success") {
            addLog(`OK: ${r.query} — ${r.rows} linhas, ${r.columns} colunas → ${r.file}`);
          } else {
            addLog(`ERRO: ${r.query} — ${r.message}`);
          }
        }
        toast.success("Extração concluída!", {
          description: `${result.results.filter((r) => r.status === "success").length} consultas extraídas`,
        });
      } else {
        addLog("Extração falhou");
        toast.error("Extração falhou");
      }
    } catch (err: any) {
      addLog(`Erro: ${err.message}`);
      toast.error("Erro na extração", { description: err.message });
    } finally {
      setIsExtracting(false);
      setProgress(100);
    }
  };

  const handleExtract = async () => {
    if (cnpj && cnpj.replace(/\D/g, "").length !== 14) {
      toast.error("CNPJ inválido", { description: "Informe um CNPJ completo, ou deixe em branco se a consulta não exigir." });
      return;
    }
    if (selectedQueries.length === 0) {
      toast.error("Nenhuma consulta selecionada");
      return;
    }
    if (!outputDir) {
      toast.error("Pasta não selecionada");
      return;
    }

    const allRequiredParams = new Set<string>();
    selectedQueries.forEach((queryId) => {
      const q = availableQueries.find(aq => aq.id === queryId);
      if (q && q.parameters) {
        q.parameters.forEach(p => allRequiredParams.add(p));
      }
    });

    // Filter out CNPJ/CNPJ_RAIZ and DATA_LIMITE_PROCESSAMENTO — preenchidos na tela
    const paramsArray = Array.from(allRequiredParams).filter(
      p => !['cnpj', 'cnpj_raiz', 'data_limite_processamento'].includes(p.toLowerCase())
    );
    if (paramsArray.length > 0) {
      setRequiredParams(paramsArray);
      const initialParams: Record<string, string> = {};
      paramsArray.forEach(p => {
        initialParams[p] = sqlParameters[p] || "";
      });
      setSqlParameters(initialParams);
      setIsParamsDialogOpen(true);
      return;
    }

    await executeExtraction({});
  };

  const updateConnection = (field: keyof OracleConnectionConfig, value: string | number) => {
    setConnection((prev) => ({ ...prev, [field]: value }));
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="space-y-1">
          <h1 className="text-2xl font-bold tracking-tight">Extração Oracle</h1>
          <p className="text-sm text-muted-foreground">
            Conecte ao banco de dados Oracle e extraia dados por CNPJ em formato Parquet
          </p>
        </div>
        <Badge variant={isConnected ? "default" : "secondary"} className="gap-1.5">
          {isConnected ? <CheckCircle2 className="h-3 w-3" /> : <XCircle className="h-3 w-3" />}
          {isConnected ? "Conectado" : "Desconectado"}
        </Badge>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left Column - Connection & CNPJ */}
        <div className="space-y-4">
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-semibold flex items-center gap-2">
                <Database className="h-4 w-4 text-primary" />
                Conexão Oracle
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="space-y-1.5">
                <Label className="text-xs">Host</Label>
                <Input
                  value={connection.host}
                  onChange={(e) => updateConnection("host", e.target.value)}
                  className="h-8 text-xs font-mono"
                />
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1.5">
                  <Label className="text-xs">Porta</Label>
                  <Input
                    value={connection.port}
                    onChange={(e) => updateConnection("port", parseInt(e.target.value) || 1521)}
                    className="h-8 text-xs font-mono"
                  />
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs">Serviço</Label>
                  <Input
                    value={connection.service}
                    onChange={(e) => updateConnection("service", e.target.value)}
                    className="h-8 text-xs font-mono"
                  />
                </div>
              </div>
              <div className="space-y-1.5">
                <Label className="text-xs">Usuário (CPF)</Label>
                <Input
                  value={connection.user}
                  onChange={(e) => updateConnection("user", e.target.value)}
                  className="h-8 text-xs"
                />
              </div>
              <div className="space-y-1.5">
                <Label className="text-xs">Senha</Label>
                <Input
                  type="password"
                  value={connection.password}
                  onChange={(e) => updateConnection("password", e.target.value)}
                  className="h-8 text-xs font-mono"
                />
              </div>

              <div className="flex items-center space-x-2 py-1">
                <Checkbox
                  id="remember"
                  checked={rememberCredentials}
                  onCheckedChange={(checked) => setRememberCredentials(checked as boolean)}
                />
                <label
                  htmlFor="remember"
                  className="text-xs font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
                >
                  Lembrar minhas credenciais
                </label>
              </div>

              <Button size="sm" className="w-full" onClick={handleTestConnection} disabled={isTesting}>
                {isTesting ? (
                  <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />
                ) : (
                  <RefreshCw className="h-3.5 w-3.5 mr-1.5" />
                )}
                {isTesting ? "Testando..." : "Testar Conexão"}
              </Button>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-semibold">CNPJ do Contribuinte</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <Input
                placeholder="00.000.000/0000-00"
                value={cnpj}
                onChange={handleCnpjChange}
                className="font-mono text-sm"
              />
              <div className="space-y-1.5">
                <Label className="text-xs">Data Limite de Processamento</Label>
                <Input
                  placeholder="DD/MM/AAAA (opcional — padrão: data atual)"
                  value={dataLimiteProcessamento}
                  onChange={(e) => setDataLimiteProcessamento(e.target.value)}
                  className="h-8 text-xs font-mono"
                />
              </div>
              <div className="space-y-1.5">
                <Label className="text-xs">Pasta de Consultas SQL</Label>
                <div className="flex gap-2">
                  <Input
                    placeholder="/caminho/para/sqls"
                    value={sqlDir}
                    onChange={(e) => setSqlDir(e.target.value)}
                    className="h-8 text-xs font-mono flex-1"
                  />
                  <Button variant="outline" size="sm" className="h-8 w-8 p-0" onClick={() => setBrowserType("sql")} aria-label="Selecionar pasta de consultas SQL" title="Selecionar pasta de consultas SQL">
                    <FolderOpen className="h-4 w-4" />
                  </Button>
                </div>
              </div>
              <div className="space-y-1.5">
                <Label className="text-xs">Pasta de Saída</Label>
                <div className="flex gap-2">
                  <Input
                    placeholder="/caminho/para/saida"
                    value={outputDir}
                    onChange={(e) => setOutputDir(e.target.value)}
                    className="h-8 text-xs font-mono flex-1"
                  />
                  <Button variant="outline" size="sm" className="h-8 w-8 p-0" onClick={() => setBrowserType("output")} aria-label="Selecionar pasta de saída" title="Selecionar pasta de saída">
                    <FolderOpen className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Center Column - Query Selection */}
        <div className="space-y-4">
          <Card className="h-full">
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <CardTitle className="text-sm font-semibold flex items-center gap-2">
                  <FileCode className="h-4 w-4 text-primary" />
                  Consultas SQL
                </CardTitle>
                <Button variant="ghost" size="sm" className="h-7 text-xs" onClick={selectAllQueries}>
                  {selectedQueries.length === availableQueries.length ? "Desmarcar Todas" : "Selecionar Todas"}
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              <ScrollArea className="h-[280px]">
                <div className="space-y-2">
                  {availableQueries.length === 0 ? (
                    <div className="flex items-center justify-center p-8 text-muted-foreground text-xs text-center">
                      Nenhuma consulta SQL encontrada. <br />
                      Selecione uma pasta com arquivos .sql na seção à esquerda.
                    </div>
                  ) : (
                    availableQueries.map((query) => (
                      <div
                        key={query.id}
                        className={`flex items-start gap-3 p-3 rounded-lg border cursor-pointer transition-colors ${selectedQueries.includes(query.id)
                          ? "border-primary/30 bg-primary/5"
                          : "border-transparent hover:bg-muted/50"
                          }`}
                        onClick={() => toggleQuery(query.id)}
                      >
                        <Checkbox checked={selectedQueries.includes(query.id)} className="mt-0.5" />
                        <div className="space-y-0.5">
                          <p className="text-sm font-medium">{query.name}</p>
                          <p className="text-xs text-muted-foreground truncate" title={query.description}>{query.description}</p>
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </ScrollArea>

              <Separator className="my-3" />

              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <Switch checked={includeAuxiliary} onCheckedChange={setIncludeAuxiliary} />
                  <Label className="text-xs">Incluir tabelas auxiliares</Label>
                </div>
                <Badge variant="outline" className="text-xs">
                  {selectedQueries.length} selecionadas
                </Badge>
              </div>

              {includeAuxiliary && auxiliaryQueries.length > 0 && (
                <div className="mt-3 p-3 rounded-lg bg-muted/30 border border-dashed">
                  <p className="text-xs font-medium text-muted-foreground mb-2">
                    Consultas auxiliares ({auxiliaryQueries.length}):
                  </p>
                  <div className="space-y-1">
                    {auxiliaryQueries.map((q) => (
                      <div key={q.id} className="flex items-center gap-2 text-xs text-muted-foreground">
                        <CheckCircle2 className="h-3 w-3 text-emerald-500" />
                        <span>{q.name}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Right Column - Execution */}
        <div className="space-y-4">
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-semibold">Executar Extração</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="p-3 rounded-lg bg-muted/50 space-y-2">
                <div className="flex justify-between text-xs">
                  <span className="text-muted-foreground">CNPJ:</span>
                  <span className="font-mono font-medium">{cnpj || "—"}</span>
                </div>
                <div className="flex justify-between text-xs">
                  <span className="text-muted-foreground">Data Limite:</span>
                  <span className="font-mono font-medium">{dataLimiteProcessamento || "Atual"}</span>
                </div>
                <div className="flex justify-between text-xs">
                  <span className="text-muted-foreground">Consultas:</span>
                  <span className="font-medium">{selectedQueries.length}</span>
                </div>
                <div className="flex justify-between text-xs">
                  <span className="text-muted-foreground">Auxiliares:</span>
                  <span className="font-medium">{includeAuxiliary ? `Sim (${auxiliaryQueries.length})` : "Não"}</span>
                </div>
                <div className="flex justify-between text-xs">
                  <span className="text-muted-foreground">Destino:</span>
                  <span className="font-mono font-medium text-right max-w-[150px] truncate">
                    {outputDir || "—"}
                  </span>
                </div>
              </div>

              <Button className="w-full" onClick={handleExtract} disabled={isExtracting}>
                {isExtracting ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <Play className="h-4 w-4 mr-2" />
                )}
                {isExtracting ? "Extraindo..." : "Iniciar Extração"}
              </Button>

              {isExtracting && (
                <div className="space-y-2">
                  <div className="flex justify-between text-xs">
                    <span className="text-muted-foreground">Progresso</span>
                    <span className="font-medium">{progress}%</span>
                  </div>
                  <Progress value={progress} />
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-semibold flex items-center gap-2">
                <AlertCircle className="h-4 w-4 text-muted-foreground" />
                Log de Execução
              </CardTitle>
            </CardHeader>
            <CardContent>
              <ScrollArea className="h-[200px]">
                {logs.length === 0 ? (
                  <div className="flex items-center justify-center h-full text-xs text-muted-foreground py-8">
                    Nenhum log disponível. Inicie uma extração.
                  </div>
                ) : (
                  <div className="space-y-1 font-mono text-xs">
                    {logs.map((log, i) => (
                      <p key={i} className="text-muted-foreground">{log}</p>
                    ))}
                  </div>
                )}
              </ScrollArea>
            </CardContent>
          </Card>
        </div>
      </div>

      <FolderBrowserDialog
        open={browserType !== null}
        onOpenChange={(open) => {
          if (!open) setBrowserType(null);
        }}
        initialPath={
          browserType === "output" ? outputDir :
            browserType === "sql" ? sqlDir :
              ""
        }
        title={
          browserType === "output" ? "Selecionar Pasta de Saída" :
            browserType === "sql" ? "Selecionar Pasta de Consultas SQL" :
              "Selecionar Pasta"
        }
        onSelect={(path) => {
          if (browserType === "output") {
            setOutputDir(path);
          } else if (browserType === "sql") {
            setSqlDir(path);
          }
        }}
      />
      <Dialog open={isParamsDialogOpen} onOpenChange={setIsParamsDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Parâmetros da Consulta</DialogTitle>
            <DialogDescription>
              Preencha os parâmetros abaixo. Campos deixados em branco usarão o valor padrão definido na consulta SQL (geralmente a data atual).
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            {requiredParams.map(param => {
              const paramLower = param.toLowerCase();
              let placeholder = `Valor para ${param}`;
              let hint = "";
              if (paramLower.includes("data") || paramLower.includes("dt_")) {
                placeholder = "DD/MM/AAAA";
                hint = "Formato: DD/MM/AAAA — deixe vazio para usar a data atual";
              }
              return (
                <div key={param} className="space-y-1.5">
                  <Label className="font-mono text-xs">{param}</Label>
                  <Input
                    value={sqlParameters[param] || ""}
                    onChange={(e) => setSqlParameters(prev => ({ ...prev, [param]: e.target.value }))}
                    placeholder={placeholder}
                    className="font-mono"
                  />
                  {hint && <p className="text-xs text-muted-foreground">{hint}</p>}
                </div>
              );
            })}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setIsParamsDialogOpen(false)}>Cancelar</Button>
            <Button onClick={() => executeExtraction(sqlParameters)}>Confirmar e Extrair</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
