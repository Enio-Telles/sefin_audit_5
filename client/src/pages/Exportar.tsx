import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  FileSpreadsheet,
  Search,
  Download,
  FileCheck,
  Loader2,
  FileUp,
} from "lucide-react";
import { useState, useCallback } from "react";
import { toast } from "sonner";
import { listParquetFiles, exportToExcel, downloadExcel, type ParquetFileInfo } from "@/lib/pythonApi";

export default function Exportar() {
  const [sourceDir, setSourceDir] = useState("");
  const [selectedFiles, setSelectedFiles] = useState<string[]>([]);
  const [files, setFiles] = useState<ParquetFileInfo[]>([]);
  const [isExporting, setIsExporting] = useState(false);
  const [isLoading, setIsLoading] = useState(false);

  const handleLoadFiles = useCallback(async () => {
    if (!sourceDir.trim()) {
      toast.error("Informe o diretório de origem");
      return;
    }
    setIsLoading(true);
    try {
      const result = await listParquetFiles(sourceDir);
      setFiles(result.files);
      setSelectedFiles([]);
      if (result.files.length === 0) {
        toast.info("Nenhum arquivo Parquet encontrado");
      } else {
        toast.success(`${result.count} arquivo(s) encontrado(s)`);
      }
    } catch (err: any) {
      toast.error("Erro ao listar arquivos", { description: err.message });
    } finally {
      setIsLoading(false);
    }
  }, [sourceDir]);

  const toggleFile = (path: string) => {
    setSelectedFiles((prev) =>
      prev.includes(path) ? prev.filter((f) => f !== path) : [...prev, path]
    );
  };

  const selectAll = () => {
    if (selectedFiles.length === files.length) {
      setSelectedFiles([]);
    } else {
      setSelectedFiles(files.map((f) => f.path));
    }
  };

  const handleExport = useCallback(async () => {
    if (selectedFiles.length === 0) {
      toast.error("Nenhum arquivo selecionado");
      return;
    }
    setIsExporting(true);
    try {
      const result = await exportToExcel(selectedFiles, sourceDir);
      const successCount = result.results.filter((r) => r.status === "success").length;
      toast.success(`${successCount} arquivo(s) exportado(s) com sucesso!`);
    } catch (err: any) {
      toast.error("Erro ao exportar", { description: err.message });
    } finally {
      setIsExporting(false);
    }
  }, [selectedFiles, sourceDir]);

  const handleDownloadSingle = useCallback(async (filePath: string) => {
    try {
      await downloadExcel(filePath);
      toast.success("Download iniciado!");
    } catch (err: any) {
      toast.error("Erro no download", { description: err.message });
    }
  }, []);

  return (
    <div className="space-y-6">
      <div className="space-y-1">
        <h1 className="text-2xl font-bold tracking-tight">Exportar para Excel</h1>
        <p className="text-sm text-muted-foreground">
          Converta arquivos Parquet em planilhas Excel formatadas
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Config */}
        <div className="space-y-4">
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-semibold">Diretórios</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="space-y-1.5">
                <Label className="text-xs">Pasta de Origem (Parquet)</Label>
                <div className="flex gap-2">
                  <Input
                    placeholder="/caminho/para/parquets"
                    value={sourceDir}
                    onChange={(e) => setSourceDir(e.target.value)}
                    className="h-8 text-xs font-mono"
                    onKeyDown={(e) => e.key === "Enter" && handleLoadFiles()}
                  />
                  <Button
                    variant="outline"
                    size="sm"
                    className="shrink-0 h-8"
                    onClick={handleLoadFiles}
                    disabled={isLoading}
                    aria-label="Buscar arquivos Parquet"
                    title="Buscar arquivos Parquet"
                  >
                    {isLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Search className="h-3.5 w-3.5" />}
                  </Button>
                </div>
              </div>
              <Button className="w-full" onClick={handleExport} disabled={isExporting || selectedFiles.length === 0}>
                {isExporting ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <Download className="h-4 w-4 mr-2" />
                )}
                {isExporting ? "Exportando..." : `Exportar ${selectedFiles.length} Selecionado(s)`}
              </Button>
            </CardContent>
          </Card>
        </div>

        {/* File List */}
        <div className="lg:col-span-2">
          <Card>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <CardTitle className="text-sm font-semibold flex items-center gap-2">
                  <FileSpreadsheet className="h-4 w-4 text-primary" />
                  Arquivos Parquet Disponíveis
                </CardTitle>
                {files.length > 0 && (
                  <Button variant="ghost" size="sm" className="h-7 text-xs" onClick={selectAll}>
                    {selectedFiles.length === files.length ? "Desmarcar" : "Selecionar"} Todos
                  </Button>
                )}
              </div>
            </CardHeader>
            <CardContent>
              {files.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-12 gap-3 text-muted-foreground">
                  <FileUp className="h-10 w-10 opacity-50" />
                  <p className="text-sm">Informe um diretório e clique em buscar</p>
                </div>
              ) : (
                <>
                  <ScrollArea className="h-[400px]">
                    <div className="space-y-2">
                      {files.map((file) => (
                        <div
                          key={file.path}
                          className={`flex items-center gap-3 p-3 rounded-lg border cursor-pointer transition-colors ${
                            selectedFiles.includes(file.path)
                              ? "border-primary/30 bg-primary/5"
                              : "border-transparent hover:bg-muted/50"
                          }`}
                          onClick={() => toggleFile(file.path)}
                        >
                          <Checkbox checked={selectedFiles.includes(file.path)} />
                          <FileCheck className="h-4 w-4 text-emerald-500 shrink-0" />
                          <div className="flex-1 min-w-0">
                            <p className="text-sm font-mono truncate">{file.relative_path || file.name}</p>
                            <div className="flex gap-3 mt-1">
                              <span className="text-xs text-muted-foreground">{file.size_human}</span>
                              <span className="text-xs text-muted-foreground">{file.rows} linhas</span>
                              <span className="text-xs text-muted-foreground">{file.columns} colunas</span>
                            </div>
                          </div>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-7 text-xs shrink-0"
                            onClick={(e) => {
                              e.stopPropagation();
                              handleDownloadSingle(file.path);
                            }}
                          >
                            <Download className="h-3 w-3" />
                          </Button>
                        </div>
                      ))}
                    </div>
                  </ScrollArea>
                  <div className="mt-3 pt-3 border-t flex items-center justify-between">
                    <Badge variant="secondary" className="text-xs">
                      {selectedFiles.length} de {files.length} selecionados
                    </Badge>
                  </div>
                </>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
