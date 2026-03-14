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




@router.post("/auditoria/pipeline")
async def audit_pipeline(req: AuditPipelineRequest):
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
            
            sql_files = sorted(DIR_SQL.glob("*.sql"))
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

                    cursor.execute(sql, bind_vars)
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    df = pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(columns)}, strict=False)
                    df = normalizar_colunas(df)
                    parquet_name = f"{query_name}_{cnpj_limpo}.parquet"
                    parquet_path = dir_parquet / parquet_name
                    df.write_parquet(str(parquet_path))
                    arquivos_extraidos.append({"name": parquet_name, "path": str(parquet_path), "rows": len(rows)})
                except Exception as e:
                    erros.append(f"Extração {query_name}: {str(e)}")

        conexao.close()

        # ETAPA 2: Análises
        # ... (simplified for now to match implementation, adding imports)
        from cruzamentos.produtos.produto_unid import unificar_produtos_unidades
        from gerar_relatorio import gerar_relatorio_jinja, gerar_resumo_txt

        arquivos_analises = []
        arquivos_produtos = []

        try:
            # 1. Unificação Master (único pipeline de produtos — produto_unid.py)
            df_unid = unificar_produtos_unidades(cnpj_limpo)

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
                if not base_detalhes_path.exists():
                    fontes_txt = ", ".join([f"{n}:{r}" for n, r in fontes_com_dados])
                    raise HTTPException(
                        status_code=422,
                        detail=f"Unificação de produtos não gerou base_detalhes. Fontes com dados: {fontes_txt}",
                    )

                base_rows = pl.scan_parquet(str(base_detalhes_path)).select(pl.len()).collect().item()
                if int(base_rows) == 0:
                    fontes_txt = ", ".join([f"{n}:{r}" for n, r in fontes_com_dados])
                    raise HTTPException(
                        status_code=422,
                        detail=f"Unificação de produtos gerou 0 linhas com fontes preenchidas. Verifique mapeamento de colunas. Fontes: {fontes_txt}",
                    )
            
            # Lista de arquivos esperados na pasta de análises para a seção de produtos
            targets = [
                (f"produtos_agregados_{cnpj_limpo}.parquet", "Tabela Visão"),
                (f"base_detalhes_produtos_{cnpj_limpo}.parquet", "Base Detalhes"),
                (f"produtos_indexados_{cnpj_limpo}.parquet", "Produtos Indexados"),
                (f"codigos_multidescricao_{cnpj_limpo}.parquet", "Códigos Multidescrição"),
                (f"variacoes_produtos_{cnpj_limpo}.parquet", "Variações Encontradas"),
                (f"mapa_manual_descricoes_{cnpj_limpo}.parquet", "Mapa Manual de Descrições"),
                (f"mapa_auditoria_descricoes_{cnpj_limpo}.parquet", "Auditoria de Descrições"),
                (f"mapa_auditoria_descricoes_aplicadas_{cnpj_limpo}.parquet", "Descrições Aplicadas"),
                (f"mapa_auditoria_descricoes_bloqueadas_{cnpj_limpo}.parquet", "Descrições Bloqueadas"),
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

        return {
            "success": True, 
            "cnpj": cnpj_limpo, 
            "arquivos_extraidos": arquivos_extraidos,
            "arquivos_analises": arquivos_analises,
            "arquivos_produtos": arquivos_produtos,
            "arquivos_relatorios": arquivos_relatorios,
            "erros": erros,
            "dir_parquet": str(dir_parquet),
            "dir_analises": str(dir_analises),
            "dir_relatorios": str(dir_relatorios),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[pipeline] Erro: %s\n%s", e, traceback.format_exc())
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
