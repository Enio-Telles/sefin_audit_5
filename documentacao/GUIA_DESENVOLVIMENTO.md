# Guia de Desenvolvimento - Adicionando Novas Análises 🚀

Este guia passo-a-passo explica como adicionar uma **nova análise/cruzamento** ao SEFIN Audit Tool.

---

## 📋 Checklist Rápido

- [ ] Criar pasta `cruzamentos/minha_analise/`
- [ ] Implementar lógica Python
- [ ] Criar endpoint FastAPI em `server/python/api.py`
- [ ] Adicionar rota tRPC em `server/routers.ts`
- [ ] Criar componentes React
- [ ] Adicionar documentação
- [ ] Testar fluxo completo
- [ ] Documentar em `DOCUMENTACAO.md`

---

## 🏗️ Passo 1: Estrutura de Pastas

### Criar a Pasta

```bash
mkdir -p cruzamentos/minha_analise
cd cruzamentos/minha_analise
```

### Criar Arquivos Base

```bash
touch __init__.py
touch documentacao_tecnica.py    # Script principal
touch helpers.py                  # Funções auxiliares
touch DOCUMENTACAO.md             # Explicação técnica
```

### Estrutura Recomendada

```
cruzamentos/minha_analise/
├── __init__.py                   # Exports públicos
├── documentacao_tecnica.py       # Função executar_analise(...)
│   └─ def executar_analise_minha_analise(cnpj, data_ini, data_fim, ...)
├── helpers.py                    # Funções auxiliares internas
│   ├─ def carregar_dados_minha_analise(cnpj)
│   ├─ def processar_dados(df)
│   └─ def calcular_resultados(df)
├── DOCUMENTACAO.md               # Explicação para auditores
└── __pycache__/
```

### Exemplo: `__init__.py`

```python
"""Módulo de Análise: Minha Análise"""

from .documentacao_tecnica import executar_analise_minha_analise

__all__ = ["executar_analise_minha_analise"]
```

---

## 🐍 Passo 2: Implementar Lógica Python

### Template: `documentacao_tecnica.py`

```python
"""
Análise de [Seu Nome de Análise]

Descrição:
---------
Esta análise [descreva o que faz]

Exemplo de uso:
---------------
>>> from minha_analise import executar_analise_minha_analise
>>> resultado = executar_analise_minha_analise(
...     cnpj="12345678000190",
...     data_ini="2024-01-01",
...     data_fim="2024-12-31",
...     input_dir="C:\\dados",
...     output_dir="C:\\relatorios"
... )
>>> print(resultado.columns)
"""

from pathlib import Path
import polars as pl
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger("minha_analise")


def executar_analise_minha_analise(
    cnpj: str,
    data_ini: Optional[str] = None,
    data_fim: Optional[str] = None,
    input_dir: str = ".",
    output_dir: str = ".",
    arquivo_entrada: Optional[str] = "nfe_saida.parquet"
) -> Dict[str, Any]:
    """
    Executa análise de Sua Análise.
    
    Args:
        cnpj: CNPJ limpo (14 dígitos)
        data_ini: Data inicial (YYYY-MM-DD)
        data_fim: Data final (YYYY-MM-DD)
        input_dir: Diretório com Parquets de entrada
        output_dir: Diretório para salvar resultados
        arquivo_entrada: Nome do arquivo Parquet principal
    
    Returns:
        Dict com resultado da análise:
        {
            "success": True,
            "cnpj": "...",
            "periodo": "...",
            "arquivo_saida": "...",
            "registros_processados": 1250,
            "resumo": {...}
        }
    """
    
    try:
        logger.info(f"[Minha Análise] Iniciando para CNPJ {cnpj}")
        
        # 1. Carrega dados
        df = _carregar_dados(cnpj, input_dir, arquivo_entrada)
        logger.info(f"[Minha Análise] Carregado {len(df)} registros")
        
        # 2. Filtra por período
        if data_ini and data_fim:
            df = _filtrar_por_periodo(df, data_ini, data_fim)
            logger.info(f"[Minha Análise] Após filtro: {len(df)} registros")
        
        # 3. Executa processamento
        df = _processar_dados(df)
        
        # 4. Calcula resultados
        df_resultado = _calcular_resultados(df)
        
        # 5. Salva resultado
        output_path = Path(output_dir) / f"analise_minha_analise_{cnpj}.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df_resultado.write_parquet(str(output_path))
        logger.info(f"[Minha Análise] Salvo em {output_path}")
        
        # 6. Retorna resumo
        return {
            "success": True,
            "cnpj": cnpj,
            "periodo": f"{data_ini} to {data_fim}" if data_ini else "sem filtro",
            "arquivo_saida": str(output_path),
            "registros_processados": len(df),
            "registros_resultado": len(df_resultado),
            "resumo": {
                "total": len(df_resultado),
                "alertas": len(df_resultado.filter(pl.col("status") == "ALERTA")),
                "conformidade": 100.0 - (len(df_resultado.filter(pl.col("status") == "ALERTA")) / len(df_resultado) * 100)
            }
        }
    
    except Exception as e:
        logger.error(f"[Minha Análise] Erro: {e}", exc_info=True)
        raise


# ============================================================
# Funções Internas (helpers)
# ============================================================

def _carregar_dados(cnpj: str, input_dir: str, arquivo_entrada: str) -> pl.DataFrame:
    """Carrega arquivo Parquet principal."""
    input_path = Path(input_dir) / f"{arquivo_entrada.replace('.parquet', '')}_{cnpj}.parquet"
    
    if not input_path.exists():
        # Fallback: tenta sem CNPJ
        input_path = Path(input_dir) / arquivo_entrada
    
    if not input_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {input_path}")
    
    df = pl.read_parquet(str(input_path))
    
    # Normaliza nomes de colunas
    df = df.rename({c: c.lower() for c in df.columns})
    
    return df


def _filtrar_por_periodo(df: pl.DataFrame, data_ini: str, data_fim: str) -> pl.DataFrame:
    """Filtra por período (coluna de data deve existir)."""
    
    # Identifica coluna de data (tenta varios nomes comuns)
    data_col = None
    for col_name in ["data_saida", "data_emissao", "data", "dt_emissao"]:
        if col_name in df.columns:
            data_col = col_name
            break
    
    if not data_col:
        logger.warning("[Minha Análise] Nenhuma coluna de data encontrada, ignorando filtro")
        return df
    
    df = df.filter(
        (pl.col(data_col) >= data_ini) &
        (pl.col(data_col) <= data_fim)
    )
    
    return df


def _processar_dados(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica lógica de processamento específica da análise."""
    
    # CUSTOMIZE AQUI: adicione suas transformações
    # Exemplo:
    df = df.with_columns([
        pl.col("valor").cast(pl.Float64),
        pl.col("quantidade").cast(pl.Int32)
    ])
    
    return df


def _calcular_resultados(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula colunas finais e status."""
    
    # CUSTOMIZE AQUI: adicione suas fórmulas
    # Exemplo:
    df = df.with_columns([
        (pl.col("valor") * 0.18).alias("icms_aproximado"),
        pl.when(pl.col("quantidade") > 100)
            .then(pl.lit("OK"))
            .otherwise(pl.lit("ALERTA"))
            .alias("status")
    ])
    
    # Seleciona colunas finais (importante!)
    df = df.select([
        "chave", "produto", "quantidade", "valor", 
        "icms_aproximado", "status"
    ])
    
    return df
```

---

## 🔌 Passo 3: Adicionar Endpoint FastAPI

Edite `server/python/api.py`:

```python
# server/python/api.py - adicione ao final, antes de rodar

# Importe sua análise
from pathlib import Path
import sys
_CRUZAMENTOS_DIR = Path(__file__).resolve().parent.parent.parent / "cruzamentos"
if str(_CRUZAMENTOS_DIR) not in sys.path:
    sys.path.insert(0, str(_CRUZAMENTOS_DIR))

from minha_analise import executar_analise_minha_analise


# Schema Pydantic para validação
class MinhaAnaliseRequest(BaseModel):
    cnpj: str
    data_ini: Optional[str] = None
    data_fim: Optional[str] = None
    input_dir: str
    output_dir: str
    arquivo_entrada: Optional[str] = "nfe_saida.parquet"


# Endpoint FastAPI
@app.post("/api/python/analytics/minha_analise")
async def analytics_minha_analise(request: MinhaAnaliseRequest):
    """Executa análise de Minha Análise."""
    try:
        resultado = executar_analise_minha_analise(
            cnpj=request.cnpj,
            data_ini=request.data_ini,
            data_fim=request.data_fim,
            input_dir=request.input_dir,
            output_dir=request.output_dir,
            arquivo_entrada=request.arquivo_entrada or "nfe_saida.parquet"
        )
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
```

---

## 📡 Passo 4: Adicionar Rota tRPC

Edite `server/routers.ts`:

```typescript
// server/routers.ts

import { z } from "zod";

// Schema de validação
const MinhaAnaliseSchema = z.object({
  cnpj: z.string().regex(/^\d{14}$/),
  data_ini: z.string().optional(),
  data_fim: z.string().optional(),
  input_dir: z.string(),
  output_dir: z.string(),
  arquivo_entrada: z.string().optional(),
});

// Adicione ao router existente
export const appRouter = router({
  // ... rotas existentes ...
  
  analytics: router({
    // ... análises existentes ...
    
    minhaAnalise: protectedProcedure
      .input(MinhaAnaliseSchema)
      .mutation(async ({ input }) => {
        const response = await fetch(
          'http://localhost:8001/api/python/analytics/minha_analise',
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(input),
          }
        );
        
        if (!response.ok) {
          const error = await response.text();
          throw new TRPCError({
            code: 'INTERNAL_SERVER_ERROR',
            message: error,
          });
        }
        
        return await response.json();
      }),
  }),
});
```

---

## ⚛️ Passo 5: Criar Componente React

Crie `client/src/pages/AnaliseMinhaAnalise.tsx`:

```tsx
import { useState } from "react";
import { api } from "@/lib/trpc";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { DataTable } from "@/components/DataTable";

export default function AnaliseMinhaAnalise() {
  const [cnpj, setCnpj] = useState("");
  const [dataIni, setDataIni] = useState("2024-01-01");
  const [dataFim, setDataFim] = useState("2024-12-31");
  const [inputDir, setInputDir] = useState("C:\\dados");
  const [outputDir, setOutputDir] = useState("C:\\relatorios");

  const { mutate: executar, isPending, data, error } =
    api.analytics.minhaAnalise.useMutation();

  const handleExecutar = () => {
    executar({
      cnpj,
      data_ini: dataIni,
      data_fim: dataFim,
      input_dir: inputDir,
      output_dir: outputDir,
    });
  };

  return (
    <div className="p-6">
      <h1 className="text-3xl font-bold mb-6">Minha Análise</h1>

      {/* Formulário */}
      <div className="bg-white p-4 rounded-lg shadow mb-6">
        <div className="grid grid-cols-2 gap-4">
          <Input
            label="CNPJ"
            value={cnpj}
            onChange={(e) => setCnpj(e.target.value)}
            placeholder="12345678000190"
          />
          <Input
            label="Data Inicial"
            type="date"
            value={dataIni}
            onChange={(e) => setDataIni(e.target.value)}
          />
          <Input
            label="Data Final"
            type="date"
            value={dataFim}
            onChange={(e) => setDataFim(e.target.value)}
          />
          <Input
            label="Dir. Entrada"
            value={inputDir}
            onChange={(e) => setInputDir(e.target.value)}
          />
          <Input
            label="Dir. Saída"
            value={outputDir}
            onChange={(e) => setOutputDir(e.target.value)}
          />
        </div>

        <Button
          onClick={handleExecutar}
          disabled={isPending || !cnpj}
          className="mt-4"
        >
          {isPending ? "⏳ Processando..." : "🚀 Executar Análise"}
        </Button>
      </div>

      {/* Status */}
      {isPending && <p className="text-blue-600">⏳ Processando análise...</p>}
      {error && <p className="text-red-600">❌ {error.message}</p>}

      {/* Resultados */}
      {data && (
        <div className="bg-white p-4 rounded-lg shadow">
          <h2 className="text-xl font-bold mb-4">Resultados</h2>

          <div className="grid grid-cols-2 gap-4 mb-4">
            <div>
              <p className="text-gray-600">Registros Processados</p>
              <p className="text-2xl font-bold">{data.registros_processados}</p>
            </div>
            <div>
              <p className="text-gray-600">Conformidade</p>
              <p className="text-2xl font-bold">
                {data.resumo.conformidade.toFixed(1)}%
              </p>
            </div>
            <div>
              <p className="text-gray-600">Alertas</p>
              <p className="text-2xl font-bold text-red-600">
                {data.resumo.alertas}
              </p>
            </div>
          </div>

          <p className="text-sm text-gray-600">
            ✓ Resultado salvo em: {data.arquivo_saida}
          </p>
        </div>
      )}
    </div>
  );
}
```

### Adicionar Rota em App.tsx

```tsx
// client/src/App.tsx

import AnaliseMinhaAnalise from "../pages/AnaliseMinhaAnalise";

function Router() {
  return (
    <Switch>
      {/* ... rotas existentes ... */}
      <Route path="/analises/minha_analise" component={AnaliseMinhaAnalise} />
    </Switch>
  );
}
```

---

## 📝 Passo 6: Documentar

Crie `cruzamentos/minha_analise/DOCUMENTACAO.md`:

```markdown
# Análise: [Nome da Análise] 📊

## O Que É?
Explicação em linguagem de auditor fiscal sobre o que a análise faz.

## Por Que é Importante?
Motivação e benefícios.

## Como Funciona?

### Entrada
- Arquivo(s) Parquet necessário(s)
- Parâmetros de entrada

### Processo
1. Passo 1
2. Passo 2
3. ...

### Saída
Descrição das colunas de resultado.

## Exemplos de Uso

### Via Python
\`\`\`python
from minha_analise import executar_analise_minha_analise
\`\`\`

### Via API
\`\`\`bash
POST /api/python/analytics/minha_analise
\`\`\`

### Via Interface Web
Menu → Análises → Minha Análise

## Referências Técnicas
[Links para documentation, papers, legislação]
```

---

## ✅ Passo 7: Testes

### Teste Local (Python)

```bash
cd cruzamentos/minha_analise
python -c "
from documentacao_tecnica import executar_analise_minha_analise
resultado = executar_analise_minha_analise(
    cnpj='12345678000190',
    input_dir='C:\\dados',
    output_dir='C:\\temp'
)
print(resultado)
"
```

### Teste API (FastAPI)

```bash
# Terminal 1
cd server/python
python -m uvicorn api:app --reload

# Terminal 2: Acessa http://localhost:8001/docs
# Testa POST /api/python/analytics/minha_analise
```

### Teste Integrado (tRPC → React)

```bash
# Terminal 1
pnpm dev

# Terminal 2  
cd server/python
python -m uvicorn api:app --port 8001 --reload

# Mude para http://localhost:3000/analises/minha_analise
# Clique no botão e verifique fluxo completo
```

---

## 🎯 Checklist Final

- [ ] Pasta `cruzamentos/minha_analise/` criada
- [ ] `__init__.py` exporta função principal
- [ ] `documentacao_tecnica.py` implementado
- [ ] Endpoint FastAPI em `server/python/api.py`
- [ ] Rota tRPC em `server/routers.ts`
- [ ] Componente React criado
- [ ] Rota adicionada em `client/src/App.tsx`
- [ ] Documentação em `DOCUMENTACAO.md`
- [ ] Teste local em Python ✓
- [ ] Teste FastAPI com Swagger ✓
- [ ] Teste integrado no UI ✓
- [ ] Erro handling implementado
- [ ] Logs configurados

---

## 📊 Template Mínimo Completo

Se quer um template mínimo para copiar/colar:

```python
# documentacao_tecnica.py - MÍNIMO

def executar_analise_minha_analise(
    cnpj: str,
    data_ini: str = None,
    data_fim: str = None,
    input_dir: str = ".",
    output_dir: str = "."
):
    import polars as pl
    from pathlib import Path
    
    # Carrega
    df = pl.read_parquet(f"{input_dir}/dados.parquet")
    
    # Processa
    df = df.with_columns([
        (pl.col("valor") * 0.18).alias("icms")
    ])
    
    # Salva
    output_path = Path(output_dir) / f"resultado_{cnpj}.parquet"
    df.write_parquet(str(output_path))
    
    return {"success": True, "arquivo": str(output_path)}
```

---

**Ver também:**
- [ANALISES_MODULOS.md](./ANALISES_MODULOS.md) - Análises existentes
- [ARQUITETURA_INTEGRACAO.md](./ARQUITETURA_INTEGRACAO.md) - Como funciona integração
- [README.md](./README.md) - Visão geral do projeto

## 🧪 Como Rodar os Testes do Projeto

Para garantir a qualidade do código, é fundamental rodar os testes antes de submeter alterações.

### Testes do Frontend (React/TypeScript)

```bash
cd client
pnpm test
pnpm check  # Type checking
```

### Testes do Backend (Python)

```bash
PYTHONPATH=server/python python -m pytest server/python/ -m 'not e2e'
```

Certifique-se de que as dependências (`requirements-dev.txt`) estão instaladas e o ambiente virtual está ativo ao rodar os testes Python.
