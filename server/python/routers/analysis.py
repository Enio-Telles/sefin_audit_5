import re
import os
import sys
import traceback
import logging
import polars as pl
from pathlib import Path
from typing import Optional, Any
from fastapi import APIRouter, HTTPException, Query
from core.models import (
    AnaliseFaturamentoRequest, 
    AuditPipelineRequest
)
from core.factor_diagnostics import diagnosticar_fatores_conversao
from core.produto_runtime import produto_pipeline_em_modo_compatibilidade, unificar_produtos_unidades
from core.utils import validar_cnpj, ler_sql, normalizar_colunas, extrair_parametros_sql

logger = logging.getLogger("sefin_audit_python")
router = APIRouter(prefix="/api/python", tags=["analysis"])

# Get project root from environment or handle it
_PROJETO_DIR = Path(__file__).resolve().parent.parent.parent.parent

# Ensure project root is in Python path for cruzamentos imports
if str(_PROJETO_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJETO_DIR))

@router.post("/analises/analise_faturamento_periodo")
async def analise_faturamento_periodo(req: AnaliseFaturamentoRequest):
    """Soma do valor_total por ano_mes com filtros opcionais."""
    try:
        base = Path(req.input_dir)
        if not base.exists():
            raise HTTPException(status_code=404, detail="Diretório de entrada não encontrado")

        parquet_name = req.arquivo_base or "nfe_saida.parquet"
        src = base / parquet_name
        if not src.exists():
            raise HTTPException(status_code=404, detail=f"Arquivo base não encontrado: {src}")

        df = pl.read_parquet(str(src))
        cols = {c.lower(): c for c in df.columns}
        col_data = cols.get("emissao_data", "emissao_data")
        col_valor = cols.get("valor_total", "valor_total")
        col_cnpj = cols.get("cnpj_emitente", "cnpj_emitente")

        if req.cnpj and col_cnpj in df.columns:
            cnpj_limpo = re.sub(r'[^0-9]', '', req.cnpj)
            if cnpj_limpo:
                df = df.filter(pl.col(col_cnpj) == cnpj_limpo)

        if col_data in df.columns and df[col_data].dtype == pl.Utf8:
            df = df.with_columns(pl.col(col_data).str.slice(0, 10).alias(col_data))

        if req.data_ini and col_data in df.columns:
            df = df.filter(pl.col(col_data) >= pl.lit(req.data_ini))
        if req.data_fim and col_data in df.columns:
            df = df.filter(pl.col(col_data) <= pl.lit(req.data_fim))

        if col_data not in df.columns or col_valor not in df.columns:
            raise HTTPException(status_code=400, detail="Colunas esperadas não encontradas (emissao_data, valor_total).")

        out = (
            df
            .with_columns(pl.col(col_data).str.slice(0, 7).alias("ano_mes"))
            .group_by("ano_mes")
            .agg(pl.col(col_valor).sum().alias("faturamento"))
            .sort("ano_mes")
        )

        Path(req.output_dir).mkdir(parents=True, exist_ok=True)
        out_path = Path(req.output_dir) / "analise_faturamento_periodo.parquet"
        out.write_parquet(str(out_path))

        return {
            "success": True,
            "rows": out.height,
            "columns": out.width,
            "file": str(out_path),
            "sample": out.head(10).to_dicts(),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[analise_faturamento] Erro: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))





import traceback
import json
from fastapi import BackgroundTasks

async def run_audit_pipeline_bg(req: AuditPipelineRequest, cnpj_limpo: str, dir_parquet, dir_analises, dir_relatorios, dir_sql):
    try:
        import os
        from dotenv import load_dotenv
        import keyring
        import oracledb


        # ETAPA 1: Extração
        load_dotenv(dotenv_path=str(_PROJETO_DIR / ".env"), override=True)
        saved_user = os.getenv("SAVED_ORACLE_USER", "").strip().strip("'").strip('"')
        if not saved_user:
             raise HTTPException(status_code=400, detail="Credenciais Oracle ausentes.")
        saved_password = keyring.get_password("sefin_audit_tool", saved_user)
        if not saved_password:
             raise HTTPException(status_code=400, detail="Senha Oracle ausente no Cofre.")

        dsn = oracledb.makedsn("exa01-scan.sefin.ro.gov.br", 1521, service_name="sefindw")
        conexao = oracledb.connect(user=saved_user, password=saved_password, dsn=dsn)
        with conexao.cursor() as cursor:
            cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
            
            from pathlib import Path
            if not isinstance(dir_sql, Path):
                dir_sql = Path(dir_sql)
            if not dir_sql.exists() or not dir_sql.is_dir():
                raise Exception(f"Diretório SQL inválido ou não encontrado: {dir_sql}")

            sql_files = sorted(dir_sql.glob("*.sql"))
            if not sql_files:
                raise Exception(f"Nenhum arquivo .sql encontrado no diretório: {dir_sql}")
            arquivos_extraidos = []
            erros = []
            for sql_file in [f for f in sql_files if f.is_file()]:
                query_name = sql_file.stem
                try:
                    sql = ler_sql(sql_file)
                    params = extrair_parametros_sql(sql)
                    bind_vars = {p: "" for p in params}
                    for p in params:
                        if p.lower() == "cnpj": bind_vars[p] = cnpj_limpo
                        elif p.lower() == "cnpj_raiz": bind_vars[p] = cnpj_limpo[:8]
                        elif p.lower() == "data_limite_processamento" and req.data_limite_processamento:
                             bind_vars[p] = req.data_limite_processamento

                    import pyarrow as pa
                    import pyarrow.parquet as pq
                    import uuid
                    import os

                    cursor.execute(sql, bind_vars)
                    columns = [desc[0] for desc in cursor.description]

                    parquet_name = f"{query_name}_{cnpj_limpo}.parquet"
                    parquet_path = dir_parquet / parquet_name

                    tmp_parquet_path = f"{parquet_path}.tmp_{uuid.uuid4()}"
                    writer = None
                    total_rows = 0
                    try:
                        while True:
                            chunk = cursor.fetchmany(100000)
                            if not chunk: break

                            data_dict = {col.lower(): [row[i] for row in chunk] for i, col in enumerate(columns)}

                            table = pa.Table.from_pydict(data_dict)
                            if writer is None: writer = pq.ParquetWriter(tmp_parquet_path, table.schema)
                            writer.write_table(table)
                            total_rows += len(chunk)

                        if writer: writer.close()
                        elif total_rows == 0:
                            empty_cols = {col.lower(): [] for col in columns}
                            empty = pa.Table.from_pydict(empty_cols)
                            pq.write_table(empty, tmp_parquet_path)

                        os.replace(tmp_parquet_path, str(parquet_path))
                    except Exception:
                        if writer: writer.close()
                        if os.path.exists(tmp_parquet_path): os.remove(tmp_parquet_path)
                        raise

                    arquivos_extraidos.append({"name": parquet_name, "path": str(parquet_path), "rows": total_rows})
                except Exception as e:
                    erros.append(f"Extração {query_name}: {str(e)}")

        conexao.close()

        # ETAPA 2: Análises
        from gerar_relatorio import gerar_relatorio_jinja, gerar_resumo_txt

        arquivos_analises = []
        arquivos_produtos = []

        try:
            # 1. Unificação Master (único pipeline de produtos — produto_unid.py)
            df_unid = unificar_produtos_unidades(cnpj_limpo, projeto_dir=_PROJETO_DIR)

            # Guardrail: não aceitar "sucesso" com base de produtos zerada quando as fontes têm dados.
            fontes_produto = [
                dir_parquet / f"NFe_{cnpj_limpo}.parquet",
                dir_parquet / f"NFCe_{cnpj_limpo}.parquet",
                dir_parquet / f"c170_simplificada_{cnpj_limpo}.parquet",
                dir_parquet / f"reg_0200_{cnpj_limpo}.parquet",
                dir_parquet / f"bloco_h_{cnpj_limpo}.parquet",
            ]
            fontes_com_dados = []
            for fp in fontes_produto:
                if fp.exists():
                    try:
                        n_rows = pl.scan_parquet(str(fp)).select(pl.len()).collect().item()
                        if n_rows > 0:
                            fontes_com_dados.append((fp.name, int(n_rows)))
                    except Exception:
                        continue

            base_detalhes_path = dir_analises / f"base_detalhes_produtos_{cnpj_limpo}.parquet"
            if fontes_com_dados:
                if not base_detalhes_path.exists() and not produto_pipeline_em_modo_compatibilidade():
                    fontes_txt = ", ".join([f"{n}:{r}" for n, r in fontes_com_dados])
                    raise HTTPException(
                        status_code=422,
                        detail=f"Unificação de produtos não gerou base_detalhes. Fontes com dados: {fontes_txt}",
                    )

                base_rows = (
                    pl.scan_parquet(str(base_detalhes_path)).select(pl.len()).collect().item()
                    if base_detalhes_path.exists()
                    else 0
                )
                if int(base_rows) == 0 and not produto_pipeline_em_modo_compatibilidade():
                    fontes_txt = ", ".join([f"{n}:{r}" for n, r in fontes_com_dados])
                    raise HTTPException(
                        status_code=422,
                        detail=f"Unificação de produtos gerou 0 linhas com fontes preenchidas. Verifique mapeamento de colunas. Fontes: {fontes_txt}",
                    )
                if produto_pipeline_em_modo_compatibilidade() and not base_detalhes_path.exists():
                    logger.warning(
                        "[audit_pipeline] modo de compatibilidade de produtos ativo; base_detalhes nao foi regenerada para %s",
                        cnpj_limpo,
                    )
            
            # Lista de arquivos do fluxo atual de produtos para a seção de produtos
            targets = [
                (f"produtos_agregados_{cnpj_limpo}.parquet", "Tabela Final"),
                (f"base_detalhes_produtos_{cnpj_limpo}.parquet", "Base Detalhes"),
                (f"status_analise_produtos_{cnpj_limpo}.parquet", "Status de Analise"),
                (f"mapa_auditoria_agregados_{cnpj_limpo}.parquet", "Mapa de Agregados"),
                (f"mapa_auditoria_desagregados_{cnpj_limpo}.parquet", "Mapa de Desagregados"),
            ]

            for file_name, label in targets:
                p = dir_analises / file_name
                if p.exists():
                    try:
                        info = pl.scan_parquet(str(p)).collect_schema()
                        # Para pegar o número de linhas sem carregar tudo:
                        row_count = pl.scan_parquet(str(p)).select(pl.len()).collect().item()
                        
                        arquivos_produtos.append({
                            "name": file_name,
                            "path": str(p.resolve()),
                            "rows": row_count,
                            "columns": len(info.names()),
                            "analise": label
                        })
                    except:
                        arquivos_produtos.append({
                            "name": file_name,
                            "path": str(p.resolve()),
                            "analise": label
                        })

        except HTTPException:
            raise
        except Exception as e: 
            erros.append(f"Processamento de Produtos: {str(e)}")
            logger.error(f"[audit_pipeline] Erro em Produtos: {e}")

        # ETAPA 3: Relatórios
        arquivos_relatorios = []
        try:
            dir_modelos = _PROJETO_DIR / "modelos_word"
            for template in ["notificacao_monitoramento_v_2.docx", "Papel_TIMBRADO_SEFIN.docx"]:
                if (dir_modelos / template).exists():
                    res = gerar_relatorio_jinja(cnpj_limpo, dir_analises, dir_relatorios, dir_modelos, template)
                    if res: arquivos_relatorios.append(res)
            res_txt = gerar_resumo_txt(cnpj_limpo, dir_analises, dir_relatorios)
            if res_txt: arquivos_relatorios.append(res_txt)
        except Exception as e: erros.append(f"Relatórios: {str(e)}")


        status_file = dir_analises / "status_pipeline.json"
        with open(status_file, "w") as f:
            json.dump({"status": "concluido", "arquivos": len(arquivos_extraidos), "detalhes": "Verifique a aba de arquivos gerados"}, f)
    except Exception as e:
        logger.error(f"[pipeline bg] Erro: {e}\n{traceback.format_exc()}")
        status_file = dir_analises / "status_pipeline.json"
        try:
            with open(status_file, "w") as f:
                json.dump({"status": "erro", "motivo": str(e)}, f)
        except Exception:
            pass

@router.post("/auditoria/pipeline")
async def audit_pipeline(req: AuditPipelineRequest, background_tasks: BackgroundTasks):
    """Pipeline completo de auditoria."""
    # This is a very large function, might need its own helper or module if it grows more.
    # Keeping it here for now as it's the "Full Audit" analysis logic.
    cnpj_limpo = re.sub(r"[^0-9]", "", req.cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ inválido")

    import importlib.util
    from dotenv import load_dotenv
    import keyring
    import oracledb
    import json

    try:
        _config_path = _PROJETO_DIR / "config.py"
        _spec = importlib.util.spec_from_file_location("sefin_config", str(_config_path))
        _sefin_config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_sefin_config)
        obter_diretorios_cnpj = _sefin_config.obter_diretorios_cnpj
        DIR_SQL = _sefin_config.DIR_SQL
        dir_parquet, dir_analises, dir_relatorios = obter_diretorios_cnpj(cnpj_limpo)

        background_tasks.add_task(run_audit_pipeline_bg, req, cnpj_limpo, dir_parquet, dir_analises, dir_relatorios, DIR_SQL)

        return {
            "success": True,
            "cnpj": cnpj_limpo,
            "etapas": [
                {"etapa": "Extração de Dados", "status": "agendada"},
                {"etapa": "Cruzamentos e Análises", "status": "agendada"},
                {"etapa": "Análise de Produtos", "status": "agendada"},
                {"etapa": "Geração de Relatórios", "status": "agendada"}
            ],
            "arquivos_extraidos": [],
            "arquivos_analises": [],
            "arquivos_produtos": [],
            "arquivos_relatorios": [],
            "erros": [],
            "dir_parquet": str(dir_parquet),
            "dir_analises": str(dir_analises),
            "dir_relatorios": str(dir_relatorios),
            "message": "Auditoria agendada em segundo plano. Verifique o status posteriormente."
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[pipeline] Erro ao agendar: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
@router.post("/fatores/import-excel")
async def importar_fatores_excel(
    cnpj: str = Query(...),
    file: Any = None,
):
    """Importa um arquivo Excel de fatores de conversão."""
    from fastapi import UploadFile, File
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ inválido")

    try:
        import pandas as pd
        import importlib.util
        _spec = importlib.util.spec_from_file_location("sefin_config", str(_PROJETO_DIR / "config.py"))
        _config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_config)
        _, dir_analises, _ = _config.obter_diretorios_cnpj(cnpj_limpo)
        fatores_path = dir_analises / f"fatores_conversao_{cnpj_limpo}.parquet"

        if not fatores_path.exists():
            raise HTTPException(status_code=404, detail="Arquivo de fatores não encontrado.")

        content = await file.read()
        df_excel = pd.read_excel(content)
        df_excel.columns = [str(c).strip().lower() for c in df_excel.columns]
        
        required_cols = {"chave_produto", "ano_referencia", "unidade_origem", "fator"}
        if not required_cols.issubset(set(df_excel.columns)):
            raise HTTPException(status_code=400, detail="Colunas obrigatórias ausentes no Excel.")

        pl_excel = (
            pl.from_pandas(df_excel)
            .with_columns([
                pl.col("chave_produto").cast(pl.Int64),
                pl.col("ano_referencia").cast(pl.Int64),
                pl.col("unidade_origem").cast(pl.Utf8).str.strip_chars(),
                pl.col("fator").cast(pl.Float64),
            ])
            .unique(subset=["chave_produto", "ano_referencia", "unidade_origem"], keep="last")
        )

        fatores = pl.read_parquet(fatores_path)
        fatores = fatores.rename({c: c.lower() for c in fatores.columns})
        join_keys = ["chave_produto", "ano_referencia", "unidade_origem"]

        fatores_atualizados = (
            fatores.join(pl_excel.select(join_keys + ["fator"]), on=join_keys, how="left", suffix="_novo")
            .with_columns([
                pl.when(pl.col("fator_novo").is_not_null()).then(pl.col("fator_novo")).otherwise(pl.col("fator")).alias("fator_atual"),
                pl.when(pl.col("fator_novo").is_not_null()).then(pl.lit(True)).otherwise(pl.col("editado_manual").fill_null(False)).alias("editado_manual_atual"),
            ])
            .drop(["fator", "fator_novo", "editado_manual"])
            .rename({"fator_atual": "fator", "editado_manual_atual": "editado_manual"})
        )

        fatores_atualizados.write_parquet(fatores_path)
        return {"success": True, "cnpj": cnpj_limpo, "file": str(fatores_path), "registros": fatores_atualizados.height}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[importar_fatores_excel] Erro: %s", e)
        raise HTTPException(status_code=500, detail=f"Erro ao importar fatores: {e}")


@router.get("/fatores/diagnostico")
async def diagnostico_fatores_excel(cnpj: str = Query(...)):
    """Gera um diagnostico de fragilidades dos fatores de conversao."""
    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj)
    if not cnpj_limpo or not validar_cnpj(cnpj_limpo):
        raise HTTPException(status_code=400, detail="CNPJ invalido")

    try:
        import importlib.util

        _spec = importlib.util.spec_from_file_location("sefin_config", str(_PROJETO_DIR / "config.py"))
        _config = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_config)
        _, dir_analises, _ = _config.obter_diretorios_cnpj(cnpj_limpo)
        fatores_path = dir_analises / f"fatores_conversao_{cnpj_limpo}.parquet"

        if not fatores_path.exists():
            return {
                "success": True,
                "available": False,
                "cnpj": cnpj_limpo,
                "file": "",
                "stats": {
                    "total_registros": 0,
                    "produtos_unicos": 0,
                    "anos_unicos": 0,
                    "unidades_unicas": 0,
                    "editados_manual": 0,
                    "fatores_invalidos": 0,
                    "fatores_extremos_altos": 0,
                    "fatores_extremos_baixos": 0,
                    "grupos_muitas_unidades": 0,
                    "grupos_alta_variacao": 0,
                },
                "issues": [],
                "message": "Arquivo de fatores nao encontrado.",
            }

        fatores = pl.read_parquet(fatores_path)
        diagnostico = diagnosticar_fatores_conversao(fatores)
        return {
            "success": True,
            "available": True,
            "cnpj": cnpj_limpo,
            "file": str(fatores_path),
            **diagnostico,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[diagnostico_fatores_excel] Erro: %s", e)
        raise HTTPException(status_code=500, detail=f"Erro ao diagnosticar fatores: {e}")
