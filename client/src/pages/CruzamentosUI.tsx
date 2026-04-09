import { useState, useEffect, useCallback, useMemo } from "react";
import { useLocation } from "wouter";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import {
  ArrowLeft,
  GitMerge,
  Database,
  Search,
  Loader2,
  FileUp,
  Columns,
  Settings2,
  Table as TableIcon,
  Play,
  Save,
  CheckCircle2,
  XCircle
} from "lucide-react";
import {
  FolderBrowserDialog
} from "@/components/ui/folder-browser-dialog";
import {
  listParquetFiles,
  readParquet,
  mergeParquetFiles,
  type ParquetFileInfo,
  type ParquetMergeRequest
} from "@/lib/pythonApi";
import { toast } from "sonner";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Checkbox } from "@/components/ui/checkbox";

export default function CruzamentosUI() {
  const [, navigate] = useLocation();

  // State for file selection
  const [leftDir, setLeftDir] = useState("");
  const [rightDir, setRightDir] = useState("");
  const [leftFiles, setLeftFiles] = useState<ParquetFileInfo[]>([]);
  const [rightFiles, setRightFiles] = useState<ParquetFileInfo[]>([]);
  const [selectedLeftFile, setSelectedLeftFile] = useState<ParquetFileInfo | null>(null);
  const [selectedRightFile, setSelectedRightFile] = useState<ParquetFileInfo | null>(null);
  const [isLoadingFiles, setIsLoadingFiles] = useState<{ left: boolean, right: boolean }>({ left: false, right: false });

  // State for column metadata
  const [leftColumns, setLeftColumns] = useState<string[]>([]);
  const [rightColumns, setRightColumns] = useState<string[]>([]);
  const [isLoadingCols, setIsLoadingCols] = useState<{ left: boolean, right: boolean }>({ left: false, right: false });

  // Merge Config State
  const [leftOn, setLeftOn] = useState<string[]>([]);
  const [rightOn, setRightOn] = useState<string[]>([]);
  const [how, setHow] = useState<"inner" | "left" | "outer" | "cross">("inner");
  const [leftColsToKeep, setLeftColsToKeep] = useState<string[]>([]);
  const [rightColsToKeep, setRightColsToKeep] = useState<string[]>([]);
  const [outputPath, setOutputPath] = useState("");
  const [isMerging, setIsMerging] = useState(false);
  const [mergeResult, setMergeResult] = useState<{ success: boolean; message: string; path?: string } | null>(null);

  // Folder Browsers
  const [showLeftBrowser, setShowLeftBrowser] = useState(false);
  const [showRightBrowser, setShowRightBrowser] = useState(false);

  // Load files when directory changes
  const handleLoadFiles = useCallback(async (dir: string, side: 'left' | 'right') => {
    if (!dir.trim()) return;
    setIsLoadingFiles(prev => ({ ...prev, [side]: true }));
    try {
      const result = await listParquetFiles(dir);
      if (side === 'left') setLeftFiles(result.files);
      else setRightFiles(result.files);
    } catch (err: any) {
      toast.error(`Erro ao listar arquivos (${side})`, { description: err.message });
    } finally {
      setIsLoadingFiles(prev => ({ ...prev, [side]: false }));
    }
  }, []);

  // Fetch columns when file is selected
  useEffect(() => {
    if (selectedLeftFile) {
      setIsLoadingCols(prev => ({ ...prev, left: true }));
      readParquet({ file_path: selectedLeftFile.path, page: 1, page_size: 1 })
        .then(res => {
          setLeftColumns(res.columns);
          setLeftColsToKeep(res.columns);
          // Auto-select join key if generic ones exist
          const common = ["id", "cnpj", "chave_acesso", "nfe_chave"].find(c => res.columns.includes(c));
          if (common && leftOn.length === 0) setLeftOn([common]);
        })
        .finally(() => setIsLoadingCols(prev => ({ ...prev, left: false })));
    }
  }, [selectedLeftFile]);

  useEffect(() => {
    if (selectedRightFile) {
      setIsLoadingCols(prev => ({ ...prev, right: true }));
      readParquet({ file_path: selectedRightFile.path, page: 1, page_size: 1 })
        .then(res => {
          setRightColumns(res.columns);
          setRightColsToKeep(res.columns);
          const common = ["id", "cnpj", "chave_acesso", "nfe_chave"].find(c => res.columns.includes(c));
          if (common && rightOn.length === 0) setRightOn([common]);

          // Suggest output path
          if (selectedLeftFile) {
            const rightName = selectedRightFile.name.replace('.parquet', '');
            setOutputPath(`${selectedLeftFile.path.replace('.parquet', '')}_merged_${rightName}.parquet`);
          }
        })
        .finally(() => setIsLoadingCols(prev => ({ ...prev, right: false })));
    }
  }, [selectedRightFile, selectedLeftFile]);

  const handleMerge = async () => {
    if (!selectedLeftFile || !selectedRightFile || !outputPath) {
      toast.error("Preencha as configurações obrigatórias");
      return;
    }

    if (how !== 'cross' && (leftOn.length === 0 || rightOn.length === 0)) {
      toast.error("Selecione as chaves de cruzamento");
      return;
    }

    setIsMerging(true);
    setMergeResult(null);
    try {
      const request: ParquetMergeRequest = {
        file_a: selectedLeftFile.path,
        file_b: selectedRightFile.path,
        output_dir: outputPath.substring(0, outputPath.lastIndexOf("/") >= 0 ? outputPath.lastIndexOf("/") : outputPath.lastIndexOf("\\")),
        output_name: outputPath.split(/[/\\]/).pop() || "merge_result.parquet",
        how,
        on: how === "cross" ? [] : leftOn,
        columns_a: leftColsToKeep,
        columns_b: rightColsToKeep,
      };

      const res = await mergeParquetFiles(request);
      setMergeResult({ success: true, message: res.message, path: res.file_path });
      toast.success("Cruzamento concluído com sucesso!");
    } catch (err: any) {
      setMergeResult({ success: false, message: err.message });
      toast.error("Erro no cruzamento", { description: err.message });
    } finally {
      setIsMerging(false);
    }
  };

  return (
    <div className="flex flex-col h-[calc(100vh-4rem)] overflow-hidden gap-6 pb-4">
      <div className="flex items-center justify-between shrink-0">
        <div className="space-y-1">
          <h1 className="text-2xl font-bold tracking-tight flex items-center gap-2">
            <GitMerge className="h-6 w-6 text-indigo-600" />
            Cruzamentos Inteligentes
          </h1>
          <p className="text-sm text-muted-foreground">
            Una tabelas Parquet usando Polars de alta performance (Equivalente ao Join em SQL)
          </p>
        </div>
        <Button variant="ghost" onClick={() => navigate("/auditar")} className="gap-2">
          <ArrowLeft className="h-4 w-4" /> Voltar
        </Button>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6 flex-1 min-h-0 overflow-hidden">

        {/* Left Column: Configuration */}
        <div className="lg:col-span-8 flex flex-col gap-6 overflow-y-auto pr-2 pb-4">

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Left Dataset Selection */}
            <Card className="border shadow-sm">
              <CardHeader className="py-3 px-4 bg-muted/30">
                <CardTitle className="text-sm font-semibold flex items-center gap-2">
                  <Badge variant="outline" className="h-5 w-5 p-0 flex items-center justify-center bg-indigo-50 text-indigo-700">1</Badge>
                  Tabela Esquerda (Base)
                </CardTitle>
              </CardHeader>
              <CardContent className="p-4 space-y-4">
                <div className="flex items-center gap-2">
                  <Input
                    placeholder="Pasta..."
                    value={leftDir}
                    onChange={e => setLeftDir(e.target.value)}
                    className="h-8 text-xs font-mono"
                    onKeyDown={e => e.key === 'Enter' && handleLoadFiles(leftDir, 'left')}
                  />
                  <Button size="sm" variant="outline" onClick={() => setShowLeftBrowser(true)} aria-label="Procurar pasta esquerda" title="Procurar pasta esquerda"><Search className="h-3 w-3" /></Button>
                  <Button size="sm" onClick={() => handleLoadFiles(leftDir, 'left')} disabled={isLoadingFiles.left} aria-label="Carregar arquivos da esquerda" title="Carregar arquivos da esquerda">
                    {isLoadingFiles.left ? <Loader2 className="h-3 w-3 animate-spin" /> : <Play className="h-3 w-3" />}
                  </Button>
                </div>

                <ScrollArea className="h-[150px] border rounded-md p-1">
                  <div className="space-y-1">
                    {leftFiles.map(file => (
                      <div
                        key={file.path}
                        className={`p-2 rounded cursor-pointer text-xs flex items-center gap-2 transition-colors ${selectedLeftFile?.path === file.path ? 'bg-primary/10 border-primary/20 border' : 'hover:bg-muted'}`}
                        onClick={() => setSelectedLeftFile(file)}
                      >
                        <FileUp className="h-3 w-3 text-emerald-500" />
                        <span className="truncate">{file.name}</span>
                      </div>
                    ))}
                    {leftFiles.length === 0 && !isLoadingFiles.left && (
                      <div className="text-center py-10 text-muted-foreground text-[11px]">Busque uma pasta</div>
                    )}
                  </div>
                </ScrollArea>
              </CardContent>
            </Card>

            {/* Right Dataset Selection */}
            <Card className="border shadow-sm">
              <CardHeader className="py-3 px-4 bg-muted/30">
                <CardTitle className="text-sm font-semibold flex items-center gap-2">
                  <Badge variant="outline" className="h-5 w-5 p-0 flex items-center justify-center bg-indigo-50 text-indigo-700">2</Badge>
                  Tabela Direita (Cruzamento)
                </CardTitle>
              </CardHeader>
              <CardContent className="p-4 space-y-4">
                <div className="flex items-center gap-2">
                  <Input
                    placeholder="Pasta..."
                    value={rightDir}
                    onChange={e => setRightDir(e.target.value)}
                    className="h-8 text-xs font-mono"
                    onKeyDown={e => e.key === 'Enter' && handleLoadFiles(rightDir, 'right')}
                  />
                  <Button size="sm" variant="outline" onClick={() => setShowRightBrowser(true)} aria-label="Procurar pasta direita" title="Procurar pasta direita"><Search className="h-3 w-3" /></Button>
                  <Button size="sm" onClick={() => handleLoadFiles(rightDir, 'right')} disabled={isLoadingFiles.right} aria-label="Carregar arquivos da direita" title="Carregar arquivos da direita">
                    {isLoadingFiles.right ? <Loader2 className="h-3 w-3 animate-spin" /> : <Play className="h-3 w-3" />}
                  </Button>
                </div>

                <ScrollArea className="h-[150px] border rounded-md p-1">
                  <div className="space-y-1">
                    {rightFiles.map(file => (
                      <div
                        key={file.path}
                        className={`p-2 rounded cursor-pointer text-xs flex items-center gap-2 transition-colors ${selectedRightFile?.path === file.path ? 'bg-primary/10 border-primary/20 border' : 'hover:bg-muted'}`}
                        onClick={() => setSelectedRightFile(file)}
                      >
                        <FileUp className="h-3 w-3 text-amber-500" />
                        <span className="truncate">{file.name}</span>
                      </div>
                    ))}
                    {rightFiles.length === 0 && !isLoadingFiles.right && (
                      <div className="text-center py-10 text-muted-foreground text-[11px]">Busque uma pasta</div>
                    )}
                  </div>
                </ScrollArea>
              </CardContent>
            </Card>
          </div>

          {/* Join Configuration */}
          <Card className="border shadow-sm">
            <CardHeader className="py-3 px-4 bg-muted/30 border-b">
              <CardTitle className="text-sm font-semibold flex items-center gap-2">
                <Settings2 className="h-4 w-4 text-indigo-600" />
                Configurar Cruzamento (Join)
              </CardTitle>
            </CardHeader>
            <CardContent className="p-6 space-y-8">
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-bold">Tipo de Cruzamento</Label>
                  <Select value={how} onValueChange={(v: any) => setHow(v)}>
                    <SelectTrigger className="h-9">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="inner">Inner (Interseção)</SelectItem>
                      <SelectItem value="left">Left (Manter Esquerda)</SelectItem>
                      <SelectItem value="outer">Outer (União Completa)</SelectItem>
                      <SelectItem value="cross">Cross (Produto Cartesiano)</SelectItem>
                    </SelectContent>
                  </Select>
                  <p className="text-[10px] text-muted-foreground italic">
                    {how === 'inner' && "Apenas linhas que existem em AMBAS tabelas."}
                    {how === 'left' && "Todas as linhas da Esquerda, mesmo sem par na Direita."}
                    {how === 'outer' && "Todas as linhas de ambas tabelas."}
                    {how === 'cross' && "Combina todas as linhas com todas. Use com cautela!"}
                  </p>
                </div>

                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-bold">Chave Esquerda</Label>
                  <Select
                    disabled={how === 'cross' || isLoadingCols.left}
                    value={leftOn[0] || ""}
                    onValueChange={v => setLeftOn([v])}
                  >
                    <SelectTrigger className="h-9">
                      <SelectValue placeholder="Selecione..." />
                    </SelectTrigger>
                    <SelectContent>
                      {leftColumns.map(col => <SelectItem key={col} value={col}>{col}</SelectItem>)}
                    </SelectContent>
                  </Select>
                  {isLoadingCols.left && <div className="flex items-center gap-1 text-[10px]"><Loader2 className="h-3 w-3 animate-spin" /> Lendo...</div>}
                </div>

                <div className="space-y-2">
                  <Label className="text-xs uppercase tracking-wider text-muted-foreground font-bold">Chave Direita</Label>
                  <Select
                    disabled={how === 'cross' || isLoadingCols.right}
                    value={rightOn[0] || ""}
                    onValueChange={v => setRightOn([v])}
                  >
                    <SelectTrigger className="h-9">
                      <SelectValue placeholder="Selecione..." />
                    </SelectTrigger>
                    <SelectContent>
                      {rightColumns.map(col => <SelectItem key={col} value={col}>{col}</SelectItem>)}
                    </SelectContent>
                  </Select>
                  {isLoadingCols.right && <div className="flex items-center gap-1 text-[10px]"><Loader2 className="h-3 w-3 animate-spin" /> Lendo...</div>}
                </div>
              </div>

              {/* Column Selection */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-8 pt-4 border-t">
                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <Label className="text-xs font-bold text-indigo-700">Colunas da Esquerda para manter</Label>
                    <Button variant="ghost" size="sm" className="h-6 text-[10px]" onClick={() => setLeftColsToKeep(leftColumns)}>Todas</Button>
                  </div>
                  <ScrollArea className="h-[200px] border rounded p-2 bg-slate-50/50">
                    <div className="grid grid-cols-2 gap-1.5">
                      {leftColumns.map(col => (
                        <div key={col} className="flex items-center space-x-2">
                          <Checkbox
                            id={`l-${col}`}
                            checked={leftColsToKeep.includes(col)}
                            onCheckedChange={(c) => setLeftColsToKeep(prev => c ? [...prev, col] : prev.filter(x => x !== col))}
                          />
                          <Label htmlFor={`l-${col}`} className="text-[10px] font-mono truncate">{col}</Label>
                        </div>
                      ))}
                    </div>
                  </ScrollArea>
                </div>

                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <Label className="text-xs font-bold text-amber-700">Colunas da Direita para manter</Label>
                    <Button variant="ghost" size="sm" className="h-6 text-[10px]" onClick={() => setRightColsToKeep(rightColumns)}>Todas</Button>
                  </div>
                  <ScrollArea className="h-[200px] border rounded p-2 bg-slate-50/50">
                    <div className="grid grid-cols-2 gap-1.5">
                      {rightColumns.map(col => (
                        <div key={col} className="flex items-center space-x-2">
                          <Checkbox
                            id={`r-${col}`}
                            checked={rightColsToKeep.includes(col)}
                            onCheckedChange={(c) => setRightColsToKeep(prev => c ? [...prev, col] : prev.filter(x => x !== col))}
                          />
                          <Label htmlFor={`r-${col}`} className="text-[10px] font-mono truncate">{col}</Label>
                        </div>
                      ))}
                    </div>
                  </ScrollArea>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Right Column: Summary & Actions */}
        <div className="lg:col-span-4 flex flex-col gap-4">
          <Card className="border shadow-sm bg-gradient-to-b from-white to-slate-50">
            <CardHeader className="py-4">
              <CardTitle className="text-sm font-bold flex items-center gap-2">
                <Save className="h-4 w-4 text-indigo-600" />
                Saída do Arquivo
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <Label className="text-xs text-muted-foreground">Caminho para Salvar (.parquet)</Label>
                <Input
                  value={outputPath}
                  onChange={e => setOutputPath(e.target.value)}
                  className="h-9 text-xs font-mono"
                  placeholder="C:\Users\...\resultado.parquet"
                />
              </div>

              <div className="bg-white p-3 rounded-lg border border-indigo-100 space-y-2 shadow-sm">
                <div className="flex justify-between text-[11px]">
                  <span className="text-muted-foreground">Tabela A:</span>
                  <span className="font-bold truncate max-w-[150px]">{selectedLeftFile?.name || '-'}</span>
                </div>
                <div className="flex justify-between text-[11px]">
                  <span className="text-muted-foreground">Tabela B:</span>
                  <span className="font-bold truncate max-w-[150px]">{selectedRightFile?.name || '-'}</span>
                </div>
                <div className="flex justify-between text-[11px] pt-1 border-t">
                  <span className="text-muted-foreground">Total Colunas:</span>
                  <span className="font-bold text-indigo-600">{leftColsToKeep.length + rightColsToKeep.length}</span>
                </div>
              </div>

              <Button
                className="w-full h-12 text-md font-bold shadow-indigo-200 shadow-lg"
                onClick={handleMerge}
                disabled={isMerging || !selectedLeftFile || !selectedRightFile}
              >
                {isMerging ? (
                  <><Loader2 className="mr-2 h-5 w-5 animate-spin" /> Processando...</>
                ) : (
                  <><GitMerge className="mr-2 h-5 w-5" /> Iniciar Cruzamento</>
                )}
              </Button>
            </CardContent>
          </Card>

          {mergeResult && (
            <Card className={`border shadow-sm animate-in zoom-in-95 duration-300 ${mergeResult.success ? 'bg-green-50/50 border-green-200' : 'bg-red-50/50 border-red-200'}`}>
              <CardContent className="p-4">
                <div className="flex items-start gap-3">
                  {mergeResult.success ? (
                    <CheckCircle2 className="h-5 w-5 text-green-600" />
                  ) : (
                    <XCircle className="h-5 w-5 text-red-600" />
                  )}
                  <div className="space-y-1">
                    <p className={`text-sm font-bold ${mergeResult.success ? 'text-green-800' : 'text-red-800'}`}>
                      {mergeResult.success ? 'Sucesso!' : 'Erro no Cruzamento'}
                    </p>
                    <p className="text-[11px] text-muted-foreground leading-relaxed">
                      {mergeResult.message}
                    </p>
                    {mergeResult.path && (
                      <Button
                        variant="link"
                        size="sm"
                        className="h-auto p-0 text-[11px] font-mono text-indigo-600"
                        onClick={() => navigate(`/tabelas?file_path=${encodeURIComponent(mergeResult.path!)}`)}
                      >
                        Abrir arquivo gerado →
                      </Button>
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          <div className="mt-auto bg-indigo-50/50 border border-indigo-100 p-4 rounded-xl space-y-3">
            <h4 className="text-[11px] font-bold text-indigo-800 flex items-center gap-1.5 uppercase tracking-wider">
              <Columns className="h-3 w-3" />
              Por que Polars?
            </h4>
            <p className="text-[10px] text-indigo-900/70 leading-normal">
              Utilizamos o motor <strong>Polars Query Engine</strong> para processar merges em memória de forma vetorizada.
              Isso permite relacionar milhões de registros em segundos, garantindo agilidade mesmo em datasets auditados complexos.
            </p>
          </div>
        </div>
      </div>

      {/* Browsers */}
      <FolderBrowserDialog
        open={showLeftBrowser}
        onOpenChange={setShowLeftBrowser}
        onSelect={path => { setLeftDir(path); handleLoadFiles(path, 'left'); }}
      />
      <FolderBrowserDialog
        open={showRightBrowser}
        onOpenChange={setShowRightBrowser}
        onSelect={path => { setRightDir(path); handleLoadFiles(path, 'right'); }}
      />
    </div>
  );
}
