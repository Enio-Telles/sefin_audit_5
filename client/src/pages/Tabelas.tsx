import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  DropdownMenuSeparator,
} from "@/components/ui/dropdown-menu";
import { useLocation, useSearch } from "wouter";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import {
  Table2,
  FolderOpen,
  Filter,
  Plus,
  Paintbrush,
  Download,
  MoreVertical,
  Search,
  X,
  ChevronLeft,
  ChevronRight,
  ArrowUpDown,
  Trash2,
  Save,
  Loader2,
  FileUp,
  RefreshCw,
  ExternalLink,
  Database,
  Boxes,
  SplitSquareHorizontal,
} from "lucide-react";
import { useState, useMemo, useCallback, useEffect, useRef } from "react";
import { toast } from "sonner";
import { UnificarProdutosDialog } from "@/components/agrupamento/UnificarProdutosDialog";
import { DesagregarProdutosDialog } from "@/components/agrupamento/DesagregarProdutosDialog";
import {
  listParquetFiles,
  readParquet,
  writeParquetCell,
  addParquetRow,
  addParquetColumn,
  getUniqueValues,
  getProjectPaths,
  type ParquetFileInfo,
  type ParquetReadResponse,
} from "@/lib/pythonApi";
import { FolderBrowserDialog } from "@/components/ui/folder-browser-dialog";
import { useVirtualizer } from "@tanstack/react-virtual";

type ColumnStyle = {
  headerColor?: string;
  headerBg?: string;
  cellColor?: string;
  cellBg?: string;
};

type RowStyle = {
  color?: string;
  backgroundColor?: string;
};

export default function Tabelas() {
  const tableContainerRef = useRef<HTMLDivElement>(null);

  // File browser state
  const [directory, setDirectory] = useState("");
  const [files, setFiles] = useState<ParquetFileInfo[]>([]);
  const [isLoadingFiles, setIsLoadingFiles] = useState(false);
  const [selectedFile, setSelectedFile] = useState<ParquetFileInfo | null>(null);

  // Folder Browser Dialog State
  const [showFolderBrowser, setShowFolderBrowser] = useState(false);

  // Recent Folders State
  const [recentFolders, setRecentFolders] = useState<{ name: string, path: string }[]>([]);

  // Unique Values State
  const [uniqueValuesCache, setUniqueValuesCache] = useState<Record<string, string[]>>({});
  const [isLoadingUniqueValues, setIsLoadingUniqueValues] = useState<Record<string, boolean>>({});

  // Table data state
  const [tableData, setTableData] = useState<ParquetReadResponse | null>(null);
  const [isLoadingTable, setIsLoadingTable] = useState(false);
  const [localRows, setLocalRows] = useState<Record<string, unknown>[]>([]);
  const [localColumns, setLocalColumns] = useState<string[]>([]);

  // Filters and sort
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [debouncedFilters, setDebouncedFilters] = useState<Record<string, string>>({});
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc");
  
  // Pagination State
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(100);

  // Edit state
  const [editingCell, setEditingCell] = useState<{ row: number; col: string } | null>(null);
  const [editValue, setEditValue] = useState("");

  // UI state
  const [showFilterRow, setShowFilterRow] = useState(true);
  const [columnStyles, setColumnStyles] = useState<Record<string, ColumnStyle>>({});
  const [rowStyles, setRowStyles] = useState<Record<number, RowStyle>>({});

  // Debounce Filters effect
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedFilters(filters);
    }, 500);
    return () => clearTimeout(timer);
  }, [filters]);

  // Persistence Key Helper
  const getPersistenceKey = (filePath: string) => `sefin-table-prefs-${filePath.replace(/[^a-zA-Z0-9]/g, '_')}`;

  const loadPreferences = useCallback((filePath: string) => {
    try {
      const stored = localStorage.getItem(getPersistenceKey(filePath));
      if (stored) {
        const prefs = JSON.parse(stored);
        return prefs;
      }
    } catch (e) { console.error("Erro ao carregar preferências", e); }
    return null;
  }, []);

  const savePreferences = useCallback((filePath: string, visible: string[], styles: Record<string, ColumnStyle>) => {
    try {
      localStorage.setItem(getPersistenceKey(filePath), JSON.stringify({
        visibleColumns: visible,
        columnStyles: styles
      }));
    } catch (e) { }
  }, []);
  const [showAddColumnDialog, setShowAddColumnDialog] = useState(false);
  const [newColumnName, setNewColumnName] = useState("");
  const [showColorDialog, setShowColorDialog] = useState(false);
  const [colorTarget, setColorTarget] = useState<{ type: "column" | "row"; id: string | number } | null>(null);
  const [tempColor, setTempColor] = useState("#000000");
  const [tempBgColor, setTempBgColor] = useState("#ffffff");
  const [showFileBrowser, setShowFileBrowser] = useState(true);
  const [visibleColumns, setVisibleColumns] = useState<string[]>([]);
  
  // Product dialogs state
  const [activeDialog, setActiveDialog] = useState<"unificar" | "desagregar" | null>(null);
  const [selectedCodigo, setSelectedCodigo] = useState("");
  const [currentCnpj, setCurrentCnpj] = useState("");

  // Load Component Mount State
  useEffect(() => {
    try {
      const stored = localStorage.getItem("sefin-audit-recent-folders");
      if (stored) setRecentFolders(JSON.parse(stored));
    } catch { }
    const openDir = localStorage.getItem("sefin-audit-open-dir");
    if (openDir) {
      localStorage.removeItem("sefin-audit-open-dir");
      setDirectory(openDir);
      handleLoadFiles(openDir);
    }
  }, []);

  const openParquetInNewTab = (filePath: string) => {
    const url = `/tabelas/view?file_path=${encodeURIComponent(filePath)}`;
    window.open(url, "_blank");
  };

  const saveRecentFolder = useCallback((folderPath: string) => {
    if (!folderPath) return;
    setRecentFolders(prev => {
      const name = folderPath.split(/[\\/]/).filter(Boolean).pop() || folderPath;
      const newEntry = { name, path: folderPath };
      const filtered = prev.filter(f => f.path !== folderPath);
      const updated = [newEntry, ...filtered].slice(0, 10);
      localStorage.setItem("sefin-audit-recent-folders", JSON.stringify(updated));
      return updated;
    });
  }, []);

  const removeRecentFolder = useCallback((folderPath: string, e: React.MouseEvent) => {
    e.stopPropagation();
    setRecentFolders(prev => {
      const updated = prev.filter(f => f.path !== folderPath);
      localStorage.setItem("sefin-audit-recent-folders", JSON.stringify(updated));
      return updated;
    });
  }, []);

  const handleLoadFiles = useCallback(async (dirToLoad?: string) => {
    const targetDir = dirToLoad || directory;
    if (!targetDir.trim()) { toast.error("Informe um diretório"); return; }
    setDirectory(targetDir);
    setIsLoadingFiles(true);
    try {
      const result = await listParquetFiles(targetDir);
      setFiles(result.files);
      if (result.files.length === 0) toast.info("Nenhum arquivo Parquet encontrado");
      else { toast.success(`${result.count} arquivo(s) encontrado(s)`); saveRecentFolder(targetDir); }
    } catch (err: any) { toast.error("Erro ao listar arquivos", { description: err.message }); }
    finally { setIsLoadingFiles(false); }
  }, [directory, saveRecentFolder]);

  const handleLoadReferences = useCallback(async () => {
    try {
      const paths = await getProjectPaths();
      if (paths.referencias) handleLoadFiles(paths.referencias);
      else toast.error("Pasta de referências não encontrada");
    } catch (err: any) { toast.error("Erro ao buscar referências", { description: err.message }); }
  }, [handleLoadFiles]);

  const handleOpenFile = useCallback(async (file: ParquetFileInfo, resetPagination = true) => {
    setSelectedFile(file);
    setIsLoadingTable(true);
    setShowFileBrowser(false);
    
    let loadedPrefs: any = null;
    if (resetPagination) {
        setPage(1);
        setFilters({});
        setSortColumn(null);
        setColumnStyles({});
        setRowStyles({});
        loadedPrefs = loadPreferences(file.path);
        if (loadedPrefs?.columnStyles) setColumnStyles(loadedPrefs.columnStyles);
    }

    try {
      const result = await readParquet({ 
        file_path: file.path, 
        page: resetPagination ? 1 : page, 
        page_size: pageSize,
        filters: resetPagination ? {} : debouncedFilters,
        sort_column: resetPagination ? undefined : sortColumn ?? undefined,
        sort_direction: resetPagination ? "asc" : sortDirection
      });
      
      setTableData(result);
      setLocalRows(result.rows);
      setLocalColumns(result.columns);
      
      if (resetPagination) {
          if (loadedPrefs?.visibleColumns) {
              setVisibleColumns(loadedPrefs.visibleColumns);
          } else {
              setVisibleColumns(result.columns);
          }
          
          // Auto-filter for manual review if column exists
          if (result.columns.includes("requer_revisao_manual")) {
            setFilters({ "requer_revisao_manual": "true" });
            toast.info("Filtro 'Revisão Manual' aplicado automaticamente");
          }
      }
      
      // Try extracting CNPJ from path if needed for dialogs
      const cnpjMatch = file.path.match(/[/\\]([0-9]{14})[/\\]/);
      setCurrentCnpj(cnpjMatch ? cnpjMatch[1] : "");

    } catch (err: any) { 
        toast.error("Erro ao ler arquivo", { description: err.message }); 
    } finally { 
        setIsLoadingTable(false); 
    }
  }, [page, pageSize, debouncedFilters, sortColumn, sortDirection, loadPreferences]);

  // Effect to re-fetch when pagination/debouncedFilters/sort change
  useEffect(() => {
    if (selectedFile && !isLoadingTable) {
        handleOpenFile(selectedFile, false);
    }
  }, [page, pageSize, debouncedFilters, sortColumn, sortDirection]);

  // Effect to save preferences when they change
  useEffect(() => {
    if (selectedFile) {
        savePreferences(selectedFile.path, visibleColumns, columnStyles);
    }
  }, [visibleColumns, columnStyles, selectedFile, savePreferences]);

  const searchString = useSearch();

  useEffect(() => {
    const searchParams = new URLSearchParams(searchString || window.location.search);
    const rawFilePathParam = searchParams.get("file_path");
    
    if (rawFilePathParam) {
      // Normalize path (Windows \ to /)
      const filePathParam = rawFilePathParam.replace(/\\/g, '/');

      // Safety check for backend errors returning "undefined" in path
      if (filePathParam.includes("undefined") || filePathParam === "null") {
          console.error("Path inválido detectado:", filePathParam);
          toast.error("Erro no caminho do arquivo", {
              description: "O caminho fornecido é inválido (backend indisponível ou erro de processamento)."
          });
          return;
      }

      // Only fetch if it hasn't been fetched yet
      const currentSelectedPath = selectedFile?.path?.replace(/\\/g, '/');
      if (!selectedFile || currentSelectedPath !== filePathParam) {
        const fileName = filePathParam.split('/').pop() || "table.parquet";
        
        // Force hide browser to show the table immediately
        setShowFileBrowser(false);
        
        handleOpenFile({ 
          path: filePathParam, 
          name: fileName, 
          relative_path: fileName, 
          size: 0, 
          size_human: "", 
          rows: 0, 
          columns: 0, 
          modified: "" 
        });
      }
    }
  }, [handleOpenFile, searchString, selectedFile]);

  // filteredRows is now just localRows since filtering is server-side
  const filteredRows = localRows;

  // VIRTUALIZER
  const rowVirtualizer = useVirtualizer({
    count: filteredRows.length,
    getScrollElement: () => tableContainerRef.current,
    estimateSize: () => 35,
    overscan: 10,
  });

  const handleSort = (col: string) => {
    if (sortColumn === col) setSortDirection((d) => (d === "asc" ? "desc" : "asc"));
    else { setSortColumn(col); setSortDirection("asc"); }
  };

  const handleFetchUniqueValues = async (col: string) => {
    if (!selectedFile || uniqueValuesCache[col]) return;
    setIsLoadingUniqueValues(prev => ({ ...prev, [col]: true }));
    try {
      const result = await getUniqueValues(selectedFile.path, col);
      setUniqueValuesCache(prev => ({ ...prev, [col]: result.values }));
    } catch (err: any) { toast.error(`Erro ao buscar valores únicos`, { description: err.message }); }
    finally { setIsLoadingUniqueValues(prev => ({ ...prev, [col]: false })); }
  };

  const handleCellDoubleClick = (rowIdx: number, col: string) => {
    setEditingCell({ row: rowIdx, col });
    setEditValue(String(filteredRows[rowIdx]?.[col] ?? ""));
  };

  const handleCellSave = useCallback(async () => {
    if (!editingCell) return;
    const newRows = [...localRows];
    const originalRow = filteredRows[editingCell.row];
    const originalIdx = localRows.indexOf(originalRow);
    if (originalIdx >= 0) {
      newRows[originalIdx] = { ...newRows[originalIdx], [editingCell.col]: editValue };
      setLocalRows(newRows);
    }
    if (selectedFile) {
      try {
        await writeParquetCell({
          file_path: selectedFile.path,
          row_index: originalIdx >= 0 ? originalIdx : editingCell.row,
          column: editingCell.col,
          value: editValue,
        });
      } catch { }
    }
    setEditingCell(null);
  }, [editingCell, editValue, localRows, filteredRows, selectedFile]);

  const handleAddRow = useCallback(async () => {
    const newRow: Record<string, unknown> = {};
    localColumns.forEach((col) => (newRow[col] = ""));
    setLocalRows((prev) => [...prev, newRow]);
    if (selectedFile) {
      try { await addParquetRow(selectedFile.path); } catch { }
    }
    toast.success("Nova linha adicionada");
  }, [localColumns, selectedFile]);

  const handleAddColumn = useCallback(async () => {
    if (!newColumnName.trim()) return;
    const colName = newColumnName.trim().toLowerCase().replace(/\s+/g, "_");
    if (localColumns.includes(colName)) { toast.error("Coluna já existe"); return; }
    setLocalColumns((prev) => [...prev, colName]);
    setLocalRows((prev) => prev.map((row) => ({ ...row, [colName]: "" })));
    if (selectedFile) {
      try { await addParquetColumn(selectedFile.path, colName); } catch { }
    }
    setShowAddColumnDialog(false); setNewColumnName(""); toast.success(`Coluna "${colName}" adicionada`);
  }, [newColumnName, localColumns, selectedFile]);

  const handleDeleteRow = useCallback((globalIdx: number) => {
    const row = filteredRows[globalIdx];
    setLocalRows((prev) => prev.filter((r) => r !== row));
    toast.success("Linha removida");
  }, [filteredRows]);

  const openColorDialog = (type: "column" | "row", id: string | number) => {
    setColorTarget({ type, id });
    if (type === "column") {
      const style = columnStyles[id as string] || {};
      setTempColor(style.cellColor || "#000000"); setTempBgColor(style.cellBg || "#ffffff");
    } else {
      const style = rowStyles[id as number] || {};
      setTempColor(style.color || "#000000"); setTempBgColor(style.backgroundColor || "#ffffff");
    }
    setShowColorDialog(true);
  };

  const applyColor = () => {
    if (!colorTarget) return;
    if (colorTarget.type === "column") {
      setColumnStyles((prev) => ({
        ...prev,
        [colorTarget.id as string]: { ...prev[colorTarget.id as string], cellColor: tempColor, cellBg: tempBgColor === "#ffffff" ? undefined : tempBgColor },
      }));
    } else {
      setRowStyles((prev) => ({
        ...prev,
        [colorTarget.id as number]: { color: tempColor, backgroundColor: tempBgColor === "#ffffff" ? undefined : tempBgColor },
      }));
    }
    setShowColorDialog(false); toast.success("Cores aplicadas");
  };

  const handleExportCurrent = useCallback(async () => {
    if (!selectedFile) return;
    try {
      const res = await fetch(`/api/python/export/excel-download?file_path=${encodeURIComponent(selectedFile.path)}`);
      if (!res.ok) throw new Error("Falha no download");
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = selectedFile.name.replace(".parquet", ".xlsx");
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
      toast.success("Arquivo Excel exportado!");
    } catch (err: any) { toast.error("Erro ao exportar", { description: err.message }); }
  }, [selectedFile]);

  const toggleColumnVisibility = (col: string) => {
    setVisibleColumns(prev => prev.includes(col) ? prev.filter(c => c !== col) : [...prev, col]);
  };

  return (
    <div className="flex flex-col h-[calc(100vh-4rem)] overflow-hidden gap-4 pb-4">
      {/* Header Fixo */}
      <div className="flex items-center justify-between shrink-0">
        <div className="space-y-1">
          <h1 className="text-2xl font-bold tracking-tight">Visualizar Tabelas</h1>
          <p className="text-sm text-muted-foreground">
            Ler, filtrar, editar e personalizar tabelas Parquet
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => setShowFileBrowser(!showFileBrowser)}>
            <FolderOpen className="h-4 w-4 mr-1.5" />
            {showFileBrowser ? "Ocultar Arquivos" : "Abrir Arquivo"}
          </Button>
          {selectedFile && (
            <Button variant="outline" size="sm" onClick={handleExportCurrent}>
              <Download className="h-4 w-4 mr-1.5" />
              Exportar Excel
            </Button>
          )}
        </div>
      </div>

      {/* File Browser */}
      {showFileBrowser && (
        <Card className="border shadow-sm">
          <CardContent className="p-4 space-y-3">
            <div className="flex items-center gap-2">
              <Button variant="outline" onClick={() => setShowFolderBrowser(true)} title="Procurar Pasta">
                <FolderOpen className="h-4 w-4" />
              </Button>
              <Input
                placeholder="Informe o diretório com os arquivos Parquet..."
                value={directory}
                onChange={(e) => setDirectory(e.target.value)}
                className="font-mono text-sm"
                onKeyDown={(e) => e.key === "Enter" && handleLoadFiles()}
              />
              <Button onClick={() => handleLoadFiles()} disabled={isLoadingFiles}>
                {isLoadingFiles ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
              </Button>
              <Button
                variant="outline"
                className="ml-auto"
                onClick={handleLoadReferences}
                title="Tabelas de Referências"
              >
                <Database className="h-4 w-4 mr-2 text-blue-500" />
                Referências
              </Button>
            </div>

            {recentFolders.length > 0 && (
              <div className="pt-2 flex flex-wrap gap-2 items-center">
                <span className="text-xs text-muted-foreground mr-1">Recentes:</span>
                {recentFolders.map((folder, i) => (
                  <Badge
                    key={`${folder.path}-${i}`}
                    variant="secondary"
                    className="group cursor-pointer hover:bg-muted flex items-center pr-1 h-6"
                    onClick={() => handleLoadFiles(folder.path)}
                    title={folder.path}
                  >
                    <FolderOpen className="h-3 w-3 mr-1.5 opacity-50" />
                    <span className="truncate max-w-[120px]">{folder.name}</span>
                    <button className="ml-1.5 opacity-0 group-hover:opacity-100 p-0.5" onClick={(e) => removeRecentFolder(folder.path, e)}>
                      <X className="h-2.5 w-2.5" />
                    </button>
                  </Badge>
                ))}
              </div>
            )}

            {files.length > 0 && (
              <ScrollArea className="max-h-[200px]">
                <div className="space-y-1">
                  {files.map((file) => (
                    <div
                      key={file.path}
                      className={`flex items-center justify-between p-2 rounded-lg cursor-pointer transition-colors hover:bg-muted/50 group ${selectedFile?.path === file.path ? "bg-primary/5 border border-primary/20" : ""}`}
                      onClick={() => handleOpenFile(file)}
                    >
                      <div className="flex items-center gap-2 min-w-0">
                        <FileUp className="h-4 w-4 text-emerald-500 shrink-0" />
                        <span className="text-sm font-mono truncate">{file.relative_path || file.name}</span>
                      </div>
                      <div className="flex items-center gap-3 shrink-0 text-xs text-muted-foreground">
                        <span>{file.size_human}</span>
                        <span>{file.rows} linhas</span>
                        <button className="opacity-0 group-hover:opacity-100 p-1" onClick={(e) => { e.stopPropagation(); openParquetInNewTab(file.path); }}>
                          <ExternalLink className="h-3.5 w-3.5" />
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              </ScrollArea>
            )}
          </CardContent>
        </Card>
      )}

      {/* Table View */}
      {!isLoadingTable && localColumns.length > 0 && (
        <>
          <Card className="border shadow-sm flex flex-col flex-1 min-h-0 overflow-hidden">
            <CardContent className="p-3 border-b bg-muted/30 shrink-0">
              <div className="flex items-center justify-between gap-4 flex-wrap">
                <div className="flex items-center gap-2">
                  <Badge variant="outline" className="font-mono text-xs">{selectedFile?.name || "Tabela"}</Badge>
                  <Badge variant="secondary" className="text-xs">
                    {tableData?.filtered_rows ?? 0} {tableData?.filtered_rows !== tableData?.total_rows ? `de ${tableData?.total_rows}` : ""} registros
                  </Badge>
                </div>
                <div className="flex items-center gap-1.5">
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                      <Button variant="outline" size="sm" className="h-7 text-xs">
                        <Table2 className="h-3 w-3 mr-1" /> Colunas
                      </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end" className="w-56 max-h-[300px] overflow-y-auto">
                      {localColumns.map(col => (
                        <div key={col} className="flex items-center gap-2 p-2 hover:bg-muted cursor-pointer" onClick={() => toggleColumnVisibility(col)}>
                          <input type="checkbox" checked={visibleColumns.includes(col)} readOnly className="h-3.5 w-3.5" />
                          <span className="text-xs truncate">{col}</span>
                        </div>
                      ))}
                    </DropdownMenuContent>
                  </DropdownMenu>
                  <Button variant={showFilterRow ? "default" : "outline"} size="sm" className="h-7 text-xs" onClick={() => setShowFilterRow(!showFilterRow)}>
                    <Filter className="h-3 w-3 mr-1" /> Filtros
                  </Button>
                  <Button variant="outline" size="sm" className="h-7 text-xs" onClick={handleAddRow}><Plus className="h-3 w-3 mr-1" /> Linha</Button>
                  <Button variant="outline" size="sm" className="h-7 text-xs" onClick={() => { setFilters({}); setSortColumn(null); setPage(1); }}><X className="h-3 w-3 mr-1" /> Limpar</Button>
                </div>
              </div>
            </CardContent>

            <div
              ref={tableContainerRef}
              className="flex-1 overflow-auto bg-background relative"
            >
              <div style={{ height: `${rowVirtualizer.getTotalSize()}px`, width: '100%', position: 'relative' }}>
                <table className="w-full text-xs border-collapse absolute top-0 left-0" style={{ tableLayout: 'fixed' }}>
                  <thead className="sticky top-0 z-30 shadow-sm bg-muted">
                    <tr className="bg-muted">
                      <th className="px-3 py-2 text-left font-semibold text-muted-foreground w-12 border-b border-r bg-muted sticky left-0 z-40">#</th>
                      {localColumns.filter(c => visibleColumns.includes(c)).map((col) => (
                        <th
                          key={col}
                          className="px-3 py-2 text-left font-semibold text-muted-foreground border-b border-r whitespace-nowrap group bg-muted sticky top-0"
                          style={{ color: columnStyles[col]?.headerColor, backgroundColor: columnStyles[col]?.headerBg, width: '200px' }}
                        >
                          <div className="flex items-center gap-1.5 justify-between">
                            <button className="hover:text-foreground flex items-center gap-1.5 truncate" onClick={() => handleSort(col)}>
                              {col} {sortColumn === col && <ArrowUpDown className="h-3 w-3" />}
                            </button>
                            <DropdownMenu onOpenChange={(open) => { if (open) handleFetchUniqueValues(col); }}>
                              <DropdownMenuTrigger asChild>
                                <button className="opacity-0 group-hover:opacity-100 p-0.5 rounded hover:bg-background/50"><MoreVertical className="h-3 w-3" /></button>
                              </DropdownMenuTrigger>
                              <DropdownMenuContent align="start" className="w-56">
                                <DropdownMenuItem onClick={() => openColorDialog("column", col)}><Paintbrush className="h-3.5 w-3.5 mr-2" /> Cores da Coluna</DropdownMenuItem>
                                <DropdownMenuItem onClick={() => handleSort(col)}><ArrowUpDown className="h-3.5 w-3.5 mr-2" /> Ordenar</DropdownMenuItem>
                              </DropdownMenuContent>
                            </DropdownMenu>
                          </div>
                        </th>
                      ))}
                      {localColumns.includes("codigo_original") && (
                        <th className="px-3 py-2 text-center font-semibold text-muted-foreground border-b border-r bg-muted sticky right-10 z-40 w-[260px]">Ações de Revisão</th>
                      )}
                      <th className="px-3 py-2 text-center font-semibold text-muted-foreground border-b border-r bg-muted sticky right-0 z-40 w-10"></th>
                    </tr>
                    {showFilterRow && (
                      <tr className="bg-muted/80 sticky top-[33px] z-30">
                        <th className="px-3 py-1.5 border-b border-r bg-muted sticky left-0 z-40"><Search className="h-3.5 w-3.5 text-muted-foreground" /></th>
                        {localColumns.filter(c => visibleColumns.includes(c)).map((col) => (
                          <th key={col} className="px-2 py-1.5 border-b border-r bg-muted/80 font-normal">
                            <Input
                              placeholder="Filtrar..."
                              value={filters[col] || ""}
                              onChange={(e) => setFilters((prev) => ({ ...prev, [col]: e.target.value }))}
                              className="h-6 text-xs border-0 bg-background/80 rounded px-1.5"
                            />
                          </th>
                        ))}
                        {localColumns.includes("codigo_original") && (
                          <th className="px-2 py-1.5 border-b border-r bg-muted/80 sticky right-10 z-40 w-[260px]"></th>
                        )}
                        <th className="px-2 py-1.5 border-b border-r bg-muted/80 sticky right-0 z-40 w-10"></th>
                      </tr>
                    )}
                  </thead>
                  <tbody>
                    {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                      const row = filteredRows[virtualRow.index];
                      const rowIdx = virtualRow.index;
                      const rStyle = rowStyles[rowIdx] || {};

                      return (
                        <tr
                          key={virtualRow.key}
                          className="hover:bg-primary/5 even:bg-muted/10 transition-colors group absolute w-full flex"
                          style={{
                            height: `${virtualRow.size}px`,
                            transform: `translateY(${virtualRow.start}px)`,
                            color: rStyle.color,
                            backgroundColor: rStyle.backgroundColor,
                          }}
                        >
                          <td className="px-3 py-2 text-muted-foreground border-b border-r font-mono text-[11px] sticky left-0 z-20 bg-background group-even:bg-muted/30 w-12 shrink-0">
                            {rowIdx + 1}
                          </td>
                          {localColumns.filter(c => visibleColumns.includes(c)).map((col) => {
                            const isEditing = editingCell?.row === rowIdx && editingCell?.col === col;
                            const cStyle = columnStyles[col] || {};
                            return (
                              <td
                                key={col}
                                className="px-3 py-2 border-b border-r whitespace-nowrap truncate grow shrink-0 font-mono text-[11px]"
                                style={{
                                  width: '200px',
                                  color: rStyle.color || cStyle.cellColor,
                                  backgroundColor: rStyle.backgroundColor || cStyle.cellBg,
                                }}
                                onDoubleClick={() => handleCellDoubleClick(rowIdx, col)}
                              >
                                {isEditing ? (
                                  <Input
                                    value={editValue}
                                    onChange={(e) => setEditValue(e.target.value)}
                                    onBlur={handleCellSave}
                                    onKeyDown={(e) => { if (e.key === "Enter") handleCellSave(); if (e.key === "Escape") setEditingCell(null); }}
                                    className="h-5 text-xs border-primary px-1"
                                    autoFocus
                                  />
                                ) : (
                                  <span>{String(row[col] ?? "")}</span>
                                )}
                              </td>
                            );
                          })}
                          {localColumns.includes("codigo_original") && (
                            <td className="px-2 py-1 border-b border-r grow shrink-0 sticky right-10 bg-background/95 z-10 w-[260px]">
                                <div className="flex items-center gap-1.5 justify-center h-full">
                                    <Button 
                                        size="sm" 
                                        variant="outline" 
                                        className="h-7 text-[10px] gap-1 px-2 border-blue-200 text-blue-700 bg-blue-50/50 hover:bg-blue-100"
                                        onClick={() => {
                                            const codigo = String(row["codigo_original"] || row["codigo"] || "");
                                            if (!currentCnpj) { toast.error("CNPJ não detectado."); return; }
                                            window.open(`/unificar/${currentCnpj}/${codigo}`, '_blank');
                                        }}
                                    >
                                        <Boxes className="h-3 w-3" /> Consolidar
                                    </Button>
                                    <Button 
                                        size="sm" 
                                        variant="outline" 
                                        className="h-7 text-[10px] gap-1 px-2 border-purple-200 text-purple-700 bg-purple-50/50 hover:bg-purple-100"
                                        onClick={() => {
                                            const codigo = String(row["codigo_original"] || row["codigo"] || "");
                                            if (!currentCnpj) { toast.error("CNPJ não detectado."); return; }
                                            window.open(`/desagregar/${currentCnpj}/${codigo}`, '_blank');
                                        }}
                                    >
                                        <SplitSquareHorizontal className="h-3 w-3" /> Separar
                                    </Button>
                                </div>
                            </td>
                          )}
                          <td className="px-1 py-1.5 border-b border-r shrink-0 w-10 sticky right-0 bg-background/95 z-10">
                            <DropdownMenu>
                              <DropdownMenuTrigger asChild>
                                <button className="opacity-0 group-hover:opacity-100 p-0.5 rounded hover:bg-muted transition-opacity"><MoreVertical className="h-3 w-3" /></button>
                              </DropdownMenuTrigger>
                              <DropdownMenuContent align="end" className="w-56">
                                <DropdownMenuItem onClick={() => openColorDialog("row", rowIdx)}><Paintbrush className="h-3 w-3 mr-2" /> Cores da Linha</DropdownMenuItem>
                                
                                <DropdownMenuSeparator />
                                <DropdownMenuItem className="text-destructive" onClick={() => handleDeleteRow(rowIdx)}><Trash2 className="h-3 w-3 mr-2" /> Remover Linha</DropdownMenuItem>
                              </DropdownMenuContent>
                            </DropdownMenu>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="flex items-center justify-between px-4 py-2 border-t bg-muted/30 shrink-0">
              <div className="flex items-center gap-4 text-xs text-muted-foreground">
                <p>Mostrando {localRows.length} registros (Página {page} de {tableData?.total_pages || 1})</p>
                <div className="flex items-center gap-1">
                    <span>Linhas por página:</span>
                    <select 
                        className="bg-transparent border rounded px-1 h-6 focus:outline-none"
                        value={pageSize}
                        onChange={(e) => {
                            setPageSize(Number(e.target.value));
                            setPage(1);
                        }}
                    >
                        {[50, 100, 200, 500, 1000].map(v => <option key={v} value={v}>{v}</option>)}
                    </select>
                </div>
              </div>
              
              <div className="flex items-center gap-1">
                <Button 
                    variant="outline" 
                    size="icon" 
                    className="h-7 w-7" 
                    disabled={page === 1 || isLoadingTable}
                    onClick={() => setPage(p => Math.max(1, p - 1))}
                >
                    <ChevronLeft className="h-4 w-4" />
                </Button>
                <div className="flex items-center gap-1 mx-2">
                    <Input 
                        value={page}
                        onChange={(e) => {
                            const val = parseInt(e.target.value);
                            if (!isNaN(val)) setPage(val);
                        }}
                        className="h-7 w-12 text-center p-0 text-xs"
                    />
                    <span className="text-xs text-muted-foreground">/ {tableData?.total_pages || 1}</span>
                </div>
                <Button 
                    variant="outline" 
                    size="icon" 
                    className="h-7 w-7" 
                    disabled={page >= (tableData?.total_pages || 1) || isLoadingTable}
                    onClick={() => setPage(p => p + 1)}
                >
                    <ChevronRight className="h-4 w-4" />
                </Button>
              </div>
            </div>
          </Card>
        </>
      )}

      {/* Floating Action Button for Go to Top */}
      {!isLoadingTable && filteredRows.length > 50 && (
        <Button
          className="fixed bottom-6 right-6 rounded-full h-10 w-10 shadow-lg"
          size="icon"
          onClick={() => tableContainerRef.current?.scrollTo({ top: 0, behavior: 'smooth' })}
          title="Ir para o topo"
        >
          <ChevronLeft className="h-4 w-4 rotate-90" />
        </Button>
      )}

      {/* Folder Browser & Dialogs */}
      <FolderBrowserDialog
        open={showFolderBrowser}
        onOpenChange={setShowFolderBrowser}
        onSelect={(path) => { setDirectory(path); handleLoadFiles(path); setShowFolderBrowser(false); }}
        initialPath={directory}
      />

      <Dialog open={showAddColumnDialog} onOpenChange={setShowAddColumnDialog}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader><DialogTitle>Adicionar Nova Coluna</DialogTitle></DialogHeader>
          <div className="space-y-3">
            <div className="space-y-1.5">
              <Label className="text-sm">Nome da Coluna</Label>
              <Input
                placeholder="nome_da_coluna"
                value={newColumnName}
                onChange={(e) => setNewColumnName(e.target.value)}
                className="font-mono"
                onKeyDown={(e) => e.key === "Enter" && handleAddColumn()}
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowAddColumnDialog(false)}>Cancelar</Button>
            <Button onClick={handleAddColumn}>Adicionar</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={showColorDialog} onOpenChange={setShowColorDialog}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Personalizar Cores — {colorTarget?.type === "column" ? `Coluna: ${colorTarget.id}` : `Linha: ${(colorTarget?.id as number) + 1}`}</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-1.5">
              <Label className="text-sm">Cor do Texto</Label>
              <div className="flex items-center gap-2">
                <input type="color" value={tempColor} onChange={(e) => setTempColor(e.target.value)} className="h-8 w-12 rounded border cursor-pointer" />
                <Input value={tempColor} onChange={(e) => setTempColor(e.target.value)} className="font-mono text-sm flex-1" />
              </div>
            </div>
            <div className="space-y-1.5">
              <Label className="text-sm">Cor de Fundo</Label>
              <div className="flex items-center gap-2">
                <input type="color" value={tempBgColor} onChange={(e) => setTempBgColor(e.target.value)} className="h-8 w-12 rounded border cursor-pointer" />
                <Input value={tempBgColor} onChange={(e) => setTempBgColor(e.target.value)} className="font-mono text-sm flex-1" />
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowColorDialog(false)}>Cancelar</Button>
            <Button onClick={applyColor}>Aplicar</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      
      <UnificarProdutosDialog
        open={activeDialog === 'unificar'}
        onOpenChange={(open) => !open && setActiveDialog(null)}
        cnpj={currentCnpj}
        codigo={selectedCodigo}
        onSuccess={() => {
           if (selectedFile) handleOpenFile(selectedFile); // Refresh table
        }}
      />

      <DesagregarProdutosDialog
        open={activeDialog === 'desagregar'}
        onOpenChange={(open) => !open && setActiveDialog(null)}
        cnpj={currentCnpj}
        codigo={selectedCodigo}
        onSuccess={() => {
           if (selectedFile) handleOpenFile(selectedFile); // Refresh table
        }}
      />
    </div>
  );
}
