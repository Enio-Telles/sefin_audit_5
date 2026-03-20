"""
Serviço de pipeline que orquestra:
  1. Extração Oracle — executa SQLs selecionados de c:\\funcoes\\sql
  2. Geração de tabelas — executa funções de src.transformacao.analise_produtos
"""
from __future__ import annotations

import re
import sys
import importlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import polars as pl

from src.config import CNPJ_ROOT, CONSULTAS_FONTE_DIR, TABELA_PRODUTOS_DIR
from src.extracao.conexao import conectar as conectar_oracle
from src.extracao.leitor_sql import ler_sql
from src.extracao.extrair_parametros import extrair_parametros_sql

# ──────────────────────────────────────────────
# Tipos
# ──────────────────────────────────────────────
@dataclass
class ResultadoPipeline:
    """Resultado da execução do pipeline."""
    ok: bool
    cnpj: str
    mensagens: list[str] = field(default_factory=list)
    arquivos_gerados: list[str] = field(default_factory=list)
    erros: list[str] = field(default_factory=list)

# ──────────────────────────────────────────────
# Registro das tabelas disponíveis
# ──────────────────────────────────────────────
TABELAS_DISPONIVEIS: list[dict[str, str]] = [
    {
        "id": "tabela_itens_caracteristicas",
        "nome": "Tabela Itens Características",
        "descricao": "Consolida NFe, NFCe, C170, Bloco H em itens únicos normalizados",
        "modulo": "src.transformacao.analise_produtos.itens",
        "funcao": "processar_tabela_itens",
    },
    {
        "id": "tabela_descricoes",
        "nome": "Tabela Descrições (para agregação)",
        "descricao": "Agrupa produtos por descrição normalizada — base para agregação manual",
        "modulo": "src.transformacao.analise_produtos.produtos",
        "funcao": "processar_tabela_produtos",
    },
]

# ──────────────────────────────────────────────
# Serviço
# ──────────────────────────────────────────────
class ServicoExtracao:
    """Executa consultas SQL Oracle e salva os resultados como Parquet."""

    def __init__(self, consultas_dir: Path = CONSULTAS_FONTE_DIR, cnpj_root: Path = CNPJ_ROOT):
        self.consultas_dir = consultas_dir
        self.cnpj_root = cnpj_root

    def listar_consultas(self) -> list[Path]:
        """Lista todos os arquivos .sql disponíveis."""
        if not self.consultas_dir.exists():
            return []
        return sorted(
            [p for p in self.consultas_dir.iterdir() if p.is_file() and p.suffix.lower() == ".sql"],
            key=lambda p: p.name.lower(),
        )

    def pasta_cnpj(self, cnpj: str) -> Path:
        return self.cnpj_root / cnpj

    def pasta_parquets(self, cnpj: str) -> Path:
        pasta = self.pasta_cnpj(cnpj) / "arquivos_parquet"
        pasta.mkdir(parents=True, exist_ok=True)
        return pasta

    def pasta_produtos(self, cnpj: str) -> Path:
        pasta = self.pasta_cnpj(cnpj) / "analises" / "produtos"
        pasta.mkdir(parents=True, exist_ok=True)
        return pasta

    @staticmethod
    def sanitizar_cnpj(cnpj: str) -> str:
        digitos = re.sub(r"\D", "", cnpj or "")
        if len(digitos) != 14:
            raise ValueError("Informe um CNPJ com 14 dígitos.")
        return digitos

    @staticmethod
    def extrair_parametros(sql_text: str) -> set[str]:
        return extrair_parametros_sql(sql_text)

    @staticmethod
    def montar_binds(sql_text: str, valores: dict[str, Any]) -> dict[str, Any]:
        parametros = extrair_parametros_sql(sql_text)
        valores_lower = {k.lower(): v for k, v in valores.items()}
        binds: dict[str, Any] = {}
        for nome in parametros:
            binds[nome] = valores_lower.get(nome.lower())
        return binds

    def executar_consultas(
        self,
        cnpj: str,
        consultas: list[Path],
        data_limite: str | None = None,
        progresso: Callable[[str], None] | None = None,
    ) -> list[str]:
        def _msg(texto: str):
            if progresso:
                progresso(texto)

        cnpj = self.sanitizar_cnpj(cnpj)
        pasta = self.pasta_parquets(cnpj)
        arquivos: list[str] = []

        _msg("Conectando ao Oracle...")
        conn = conectar_oracle()
        if conn is None:
            raise RuntimeError("Falha ao conectar ao Oracle.")

        try:
            for sql_path in consultas:
                nome_consulta = sql_path.stem.lower()
                _msg(f"Executando {sql_path.name}...")

                sql_text = ler_sql(sql_path)
                if sql_text is None:
                    _msg(f"⚠️ Não foi possível ler {sql_path.name}")
                    continue

                valores = {"cnpj": cnpj, "data_limite_processamento": data_limite}
                binds = self.montar_binds(sql_text, valores)

                try:
                    with conn.cursor() as cursor:
                        cursor.arraysize = 50_000
                        cursor.execute(sql_text, binds)
                        colunas = [desc[0].lower() for desc in cursor.description]
                        todas_linhas = cursor.fetchall()
                    # Converter para Polars de forma robusta
                    if todas_linhas:
                        try:
                            # Tenta via lista de dicionários com inferência ampla
                            registros = [dict(zip(colunas, row)) for row in todas_linhas]
                            # Aumentamos o infer_schema_length para evitar erros em colunas com tipos mistos no início
                            df = pl.DataFrame(registros, infer_schema_length=min(len(registros), 50000))
                        except Exception as e:
                            _msg(f"  ⚠️ Falha na inferência automática: {e}. Tentando modo robusto...")
                            # Fallback 1: Criar coluna por coluna (evita que um erro em uma coluna aborte tudo)
                            dados_colunas = {}
                            for i, col_name in enumerate(colunas):
                                dados_colunas[col_name] = [row[i] for row in todas_linhas]
                            
                            try:
                                df = pl.DataFrame(dados_colunas)
                            except Exception:
                                # Fallback 2: Forçar tudo como string se ainda assim falhar
                                dados_string = {}
                                for i, col_name in enumerate(colunas):
                                    dados_string[col_name] = [str(row[i]) if row[i] is not None else None for row in todas_linhas]
                                df = pl.DataFrame(dados_string)
                    else:
                        df = pl.DataFrame({col: [] for col in colunas})

                    arquivo_saida = pasta / f"{nome_consulta}_{cnpj}.parquet"
                    df.write_parquet(arquivo_saida, compression="snappy")
                    arquivos.append(str(arquivo_saida))
                    _msg(f"✅ {sql_path.name}: {df.height:,} linhas → {arquivo_saida.name}")

                except Exception as exc:
                    _msg(f"❌ Erro em {sql_path.name}: {exc}")
        finally:
            conn.close()

        return arquivos


class ServicoTabelas:
    """Executa as funções de geração de tabelas refatoradas."""

    @staticmethod
    def listar_tabelas() -> list[dict[str, str]]:
        return TABELAS_DISPONIVEIS[:]

    @staticmethod
    def gerar_tabelas(
        cnpj: str,
        tabelas_selecionadas: list[str],
        progresso: Callable[[str], None] | None = None,
    ) -> list[str]:
        def _msg(texto: str):
            if progresso:
                progresso(texto)

        cnpj = re.sub(r"\D", "", cnpj)
        pasta_parquets = CNPJ_ROOT / cnpj / "arquivos_parquet"
        pasta_analises = CNPJ_ROOT / cnpj / "analises" / "produtos"
        pasta_analises.mkdir(parents=True, exist_ok=True)
        
        geradas: list[str] = []

        # Carregador auxiliar de parquets
        def _carregar_parquet(nome_base: str) -> pl.DataFrame | None:
            path = pasta_parquets / f"{nome_base.lower()}_{cnpj}.parquet"
            if path.exists():
                return pl.read_parquet(path)
            return None

        for tab_id in tabelas_selecionadas:
            info = next((t for t in TABELAS_DISPONIVEIS if t["id"] == tab_id), None)
            if info is None: continue

            _msg(f"Processando {info['nome']}...")
            try:
                modulo = importlib.import_module(info["modulo"])
                funcao = getattr(modulo, info["funcao"])
                
                df_resultado = None
                if tab_id == "tabela_itens_caracteristicas":
                    df_c170 = _carregar_parquet("c170")
                    df_nfe = _carregar_parquet("nfe")
                    df_bloco_h = _carregar_parquet("bloco_h")
                    df_resultado = funcao(cnpj, df_c170=df_c170, df_nfe_itens=df_nfe, df_bloco_h=df_bloco_h)
                
                elif tab_id == "tabela_descricoes":
                    # Depende da tabela de itens já gerada ou carregada em memória
                    path_itens = pasta_analises / f"tabela_itens_caracteristicas_{cnpj}.parquet"
                    if path_itens.exists():
                        df_itens = pl.read_parquet(path_itens)
                        df_resultado = funcao(df_itens)
                    else:
                        _msg(f"  ⚠️ Ignorando {info['nome']}: Tabela de itens não encontrada.")
                        continue

                if df_resultado is not None:
                    caminho_saida = pasta_analises / f"{tab_id}_{cnpj}.parquet"
                    df_resultado.write_parquet(caminho_saida, compression="snappy")
                    geradas.append(tab_id)
                    _msg(f"✅ {info['nome']} gerada: {df_resultado.height:,} linhas.")
            except Exception as exc:
                _msg(f"❌ Erro ao gerar {info['nome']}: {exc}")

        return geradas


class ServicoPipelineCompleto:
    def __init__(self):
        self.servico_extracao = ServicoExtracao()
        self.servico_tabelas = ServicoTabelas()

    def executar_completo(
        self,
        cnpj: str,
        consultas: list[Path],
        tabelas: list[str],
        data_limite: str | None = None,
        progresso: Callable[[str], None] | None = None,
    ) -> ResultadoPipeline:
        cnpj = ServicoExtracao.sanitizar_cnpj(cnpj)
        resultado = ResultadoPipeline(ok=True, cnpj=cnpj)

        def _msg(texto: str):
            resultado.mensagens.append(texto)
            if progresso: progresso(texto)

        if consultas:
            try:
                arquivos = self.servico_extracao.executar_consultas(cnpj, consultas, data_limite, _msg)
                resultado.arquivos_gerados.extend(arquivos)
            except Exception as exc:
                resultado.erros.append(str(exc))
                resultado.ok = False
                return resultado

        if tabelas:
            geradas = self.servico_tabelas.gerar_tabelas(cnpj, tabelas, _msg)
            resultado.arquivos_gerados.extend(geradas)

        return resultado
