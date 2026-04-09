import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Input } from "@/components/ui/input";
import { toast } from "sonner";
import { RefreshCw, FilterX, Download, ChevronLeft, ChevronRight, Loader2 } from "lucide-react";

type ReadResponse = {
  columns: string[];
  dtypes: Record<string, string>;
  rows: Record<string, any>[];
  total_rows: number;
  filtered_rows: number;
  page: number;
  page_size: number;
  total_pages: number;
  file_name: string;
};

export type ParquetViewerProps = {
  filePath: string;
  defaultPageSize?: number;
};

function hashKey(s: string) {
  try {
    return btoa(unescape(encodeURIComponent(s)));
  } catch {
    return encodeURIComponent(s);
  }
}

export default function ParquetViewer({ filePath, defaultPageSize = 50 }: ParquetViewerProps) {
  const storageKey = useMemo(() => `viewer::${hashKey(filePath)}`,[filePath]);
  const [columns, setColumns] = useState<string[]>([]);
  const [dtypes, setDtypes] = useState<Record<string,string>>({});
  const [rows, setRows] = useState<Record<string, any>[]>([]);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(defaultPageSize);
  const [totalPages, setTotalPages] = useState(1);
  const [totalRows, setTotalRows] = useState(0);
  const [filteredRows, setFilteredRows] = useState(0);
  const [fileName, setFileName] = useState("");
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<"asc"|"desc">("asc");
  const [loading, setLoading] = useState(false);
  const [filters, setFilters] = useState<Record<string, string>>({});

  // restore persisted state
  useEffect(() => {
    const raw = localStorage.getItem(storageKey);
    if (raw) {
      try {
        const st = JSON.parse(raw);
        if (st.pageSize) setPageSize(st.pageSize);
        if (st.sortColumn) setSortColumn(st.sortColumn);
        if (st.sortDirection) setSortDirection(st.sortDirection);
        if (st.filters) setFilters(st.filters);
      } catch {}
    }
  }, [storageKey]);

  // persist state
  useEffect(() => {
    const st = { pageSize, sortColumn, sortDirection, filters };
    localStorage.setItem(storageKey, JSON.stringify(st));
  }, [storageKey, pageSize, sortColumn, sortDirection, filters]);

  const load = useCallback(async (targetPage?: number) => {
    try {
      setLoading(true);
      const body = {
        file_path: filePath,
        page: targetPage ?? page,
        page_size: pageSize,
        filters,
        sort_column: sortColumn,
        sort_direction: sortDirection,
      } as any;
      const res = await fetch("/api/python/parquet/read",{
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(body),
      });
      const data: ReadResponse = await res.json();
      if (!res.ok) throw new Error((data as any)?.detail || "Falha ao ler Parquet");
      setColumns(data.columns);
      setDtypes(data.dtypes);
      setRows(data.rows);
      setPage(data.page);
      setPageSize(data.page_size);
      setTotalPages(data.total_pages);
      setTotalRows(data.total_rows);
      setFilteredRows(data.filtered_rows);
      setFileName(data.file_name);
    } catch (e: any) {
      toast.error(e.message || "Erro ao carregar dados");
    } finally {
      setLoading(false);
    }
  }, [filePath, page, pageSize, filters, sortColumn, sortDirection]);

  useEffect(() => { load(1); }, [filePath]);

  const onFilterChange = (col: string, val: string) => {
    setFilters(prev => ({...prev, [col]: val}));
  };

  const applyFilters = () => { load(1); };
  const clearFilters = () => { setFilters({}); setSortColumn(null); setSortDirection("asc"); setPage(1); load(1); };

  const onHeaderClick = (col: string) => {
    if (sortColumn === col) {
      setSortDirection(prev => prev === "asc" ? "desc" : "asc");
    } else {
      setSortColumn(col);
      setSortDirection("asc");
    }
    load(1);
  };

  const exportExcel = async () => {
    try {
      const url = `/api/python/export/excel-download?file_path=${encodeURIComponent(filePath)}`;
      const res = await fetch(url);
      if (!res.ok) throw new Error("Falha no download");
      const blob = await res.blob();
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = (fileName?.replace(/\.parquet$/i, "") || "arquivo") + ".xlsx";
      document.body.appendChild(a);
      a.click();
      a.remove();
    } catch (e: any) {
      toast.error(e.message || "Erro ao exportar Excel");
    }
  };

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle>{fileName || filePath}</CardTitle>
          <div className="flex gap-2">
            <Button variant="outline" onClick={() => load()} disabled={loading}>
              {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
              Recarregar
            </Button>
            <Button variant="outline" onClick={clearFilters} disabled={loading}>
              <FilterX className="w-4 h-4" />
              Limpar
            </Button>
            <Button onClick={exportExcel} disabled={loading}>
              <Download className="w-4 h-4" />
              Exportar Excel
            </Button>
          </div>
        </div>
        <div className="text-xs text-muted-foreground mt-2">
          {filteredRows} / {totalRows} linhas • {columns.length} colunas
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        {/* Filtros por coluna */}
        <div className="overflow-auto">
          <Table>
            <TableHeader>
              <TableRow>
                {columns.map(col => (
                  <TableHead key={col} className="whitespace-nowrap cursor-pointer" onClick={() => onHeaderClick(col)}>
                    {col}{sortColumn===col ? (sortDirection==="asc"?" ▲":" ▼") : ""}
                  </TableHead>
                ))}
              </TableRow>
              <TableRow>
                {columns.map(col => (
                  <TableHead key={col}>
                    <Input
                      placeholder="Filtrar..."
                      value={filters[col] || ""}
                      onChange={(e)=>onFilterChange(col, e.target.value)}
                      onKeyDown={(e)=>{ if(e.key==="Enter") applyFilters(); }}
                    />
                  </TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((r, idx) => (
                <TableRow key={idx}>
                  {columns.map(c => (
                    <TableCell key={c}>
                      {r[c]===null || r[c]===undefined ? "" : String(r[c])}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>

        {/* Paginação */}
        <div className="flex items-center justify-between text-sm">
          <div>
            Página {page} / {totalPages}
          </div>
          <div className="flex gap-2">
            <Button variant="outline" disabled={page<=1 || loading} onClick={()=>{ setPage(p=>Math.max(1,p-1)); load(page-1); }}>
              <ChevronLeft className="w-4 h-4" />
              Anterior
            </Button>
            <Button variant="outline" disabled={page>=totalPages || loading} onClick={()=>{ setPage(p=>Math.min(totalPages,p+1)); load(page+1); }}>
              Próxima
              <ChevronRight className="w-4 h-4" />
            </Button>
          </div>
          <div className="flex items-center gap-2">
            <span>Tamanho:</span>
            <Input
              className="w-20"
              type="number"
              min={10}
              value={pageSize}
              onChange={(e)=>setPageSize(parseInt(e.target.value||"50",10))}
              onBlur={()=> load(1)}
            />
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
