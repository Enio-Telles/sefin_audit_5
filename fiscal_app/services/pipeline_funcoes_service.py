"""
Serviço de pipeline que orquestra:
  1. Extração Oracle — executa SQLs selecionados de c:\\funcoes\\consultas_fonte
  2. Geração de tabelas — executa funções de c:\\funcoes\\funcoes_tabelas\\tabela_produtos

Salva:
  - Parquets brutos em  c:\\funcoes\\CNPJ\\<cnpj>\\arquivos_parquet\\
  - Tabelas finais em   c:\\funcoes\\CNPJ\\<cnpj>\\analises\\produtos\\
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import polars as pl

# ──────────────────────────────────────────────
# Paths do c:\funcoes
# ──────────────────────────────────────────────
FUNCOES_DIR = Path(r"c:\funcoes")
AUXILIARES_DIR = FUNCOES_DIR / "funcoes_auxiliares"
TABELA_PRODUTOS_DIR = FUNCOES_DIR / "funcoes_tabelas" / "tabela_produtos"
CONSULTAS_FONTE_DIR = FUNCOES_DIR / "consultas_fonte"
CNPJ_ROOT = FUNCOES_DIR / "CNPJ"

# Garante que os módulos auxiliares estejam no path
for _dir in [str(AUXILIARES_DIR), str(TABELA_PRODUTOS_DIR)]:
    if _dir not in sys.path:
        sys.path.insert(0, _dir)

# ──────────────────────────────────────────────
# Imports das funções auxiliares de c:\funcoes
# ──────────────────────────────────────────────
from conectar_oracle import conectar as conectar_oracle
from ler_sql import ler_sql
from extrair_parametros import extrair_parametros_sql


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
        "modulo": "tabela_itens_caracteristicas",
        "funcao": "gerar_tabela_itens_caracteristicas",
    },
    {
        "id": "tabela_descricoes",
        "nome": "Tabela Descrições (para agregação)",
        "descricao": "Agrupa produtos por descrição normalizada — base para agregação manual",
        "modulo": "tabela_descricoes",
        "funcao": "gerar_tabela_descricoes",
    },
    {
        "id": "tabela_codigos",
        "nome": "Tabela Códigos com Múltiplas Descrições",
        "descricao": "Identifica códigos que possuem mais de uma descrição",
        "modulo": "tabela_codigos",
        "funcao": "gerar_tabela_codigos",  # name correction below
    },
    {
        "id": "fator_conversao",
        "nome": "Fator de Conversão de Unidades",
        "descricao": "Calcula fatores de conversão anuais baseados em preços médios",
        "modulo": "fator_conversao",
        "funcao": "calcular_fator_conversao",
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
        """Lista todos os arquivos .sql disponíveis em consultas_fonte."""
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
        """Extrai bind variables do SQL usando extrair_parametros.py."""
        return extrair_parametros_sql(sql_text)

    @staticmethod
    def montar_binds(sql_text: str, valores: dict[str, Any]) -> dict[str, Any]:
        """Monta o dicionário de binds para execução Oracle."""
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
        """
        Executa as consultas SQL selecionadas contra Oracle.

        Args:
            cnpj: CNPJ numérico (14 dígitos).
            consultas: Lista de caminhos para arquivos .sql.
            data_limite: Valor para :data_limite_processamento (DD/MM/YYYY).
            progresso: Callback para mensagens de progresso.

        Returns:
            Lista de caminhos dos parquets gerados.
        """
        def _msg(texto: str):
            if progresso:
                progresso(texto)

        cnpj = self.sanitizar_cnpj(cnpj)
        pasta = self.pasta_parquets(cnpj)
        arquivos: list[str] = []

        _msg("Conectando ao Oracle...")
        conn = conectar_oracle()
        if conn is None:
            raise RuntimeError("Falha ao conectar ao Oracle. Verifique credenciais e VPN.")

        try:
            for sql_path in consultas:
                nome_consulta = sql_path.stem.lower()
                _msg(f"Executando {sql_path.name}...")

                sql_text = ler_sql(sql_path)
                if sql_text is None:
                    _msg(f"⚠️ Não foi possível ler {sql_path.name}")
                    continue

                # Montar binds automaticamente
                valores = {
                    "CNPJ": cnpj,
                    "cnpj": cnpj,
                    "data_limite_processamento": data_limite,
                    "DATA_LIMITE_PROCESSAMENTO": data_limite,
                }
                binds = self.montar_binds(sql_text, valores)

                # Executar
                try:
                    with conn.cursor() as cursor:
                        cursor.arraysize = 50_000
                        cursor.prefetchrows = 50_000
                        cursor.execute(sql_text, binds)
                        # Oracle names are normally uppercase; lowercase them for consistency
                        colunas = [desc[0].lower() for desc in cursor.description]
                        todas_linhas: list[tuple] = []
                        while True:
                            lote = cursor.fetchmany(50_000)
                            if not lote:
                                break
                            todas_linhas.extend(lote)
                            _msg(f"  {sql_path.name}: {len(todas_linhas):,} linhas lidas...")

                    # Converter para Polars e salvar
                    if todas_linhas:
                        try:
                            # Tenta inferir com um sample maior
                            registros = [dict(zip(colunas, row)) for row in todas_linhas]
                            df = pl.DataFrame(registros, infer_schema_length=min(len(registros), 50000))
                        except Exception as e:
                            _msg(f"  ⚠️ Falha na inferência automática: {e}. Tentando modo robusto...")
                            # Fallback: Cria coluna por coluna convertendo para string se necessário
                            # Isso evita o erro "could not append value" em tipos mistos
                            dados_colunas = {}
                            for i, col_name in enumerate(colunas):
                                dados_colunas[col_name] = [row[i] for row in todas_linhas]
                            
                            try:
                                df = pl.DataFrame(dados_colunas)
                            except Exception:
                                # Última tentativa: tudo como string
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
    """Executa as funções de geração de tabelas de c:\\funcoes\\funcoes_tabelas."""

    @staticmethod
    def listar_tabelas() -> list[dict[str, str]]:
        """Retorna as tabelas disponíveis para geração."""
        return TABELAS_DISPONIVEIS[:]

    @staticmethod
    def gerar_tabelas(
        cnpj: str,
        tabelas_selecionadas: list[str],
        progresso: Callable[[str], None] | None = None,
    ) -> list[str]:
        """
        Executa as funções de geração na ordem correta de dependência.

        Args:
            cnpj: CNPJ numérico.
            tabelas_selecionadas: Lista de IDs (ex: ["tabela_itens_caracteristicas", "tabela_descricoes"]).
            progresso: Callback de progresso.

        Returns:
            Lista de nomes das tabelas geradas com sucesso.
        """
        def _msg(texto: str):
            if progresso:
                progresso(texto)

        cnpj = re.sub(r"\D", "", cnpj)
        pasta_cnpj = CNPJ_ROOT / cnpj
        geradas: list[str] = []

        # Ordem de execução (respeita dependências)
        ordem = ["tabela_itens_caracteristicas", "tabela_descricoes", "tabela_codigos", "fator_conversao"]

        for tab_id in ordem:
            if tab_id not in tabelas_selecionadas:
                continue

            info = next((t for t in TABELAS_DISPONIVEIS if t["id"] == tab_id), None)
            if info is None:
                continue

            _msg(f"Gerando {info['nome']}...")
            try:
                funcao = _importar_funcao_tabela(info["modulo"], info["funcao"])
                resultado = funcao(cnpj, pasta_cnpj)
                if resultado:
                    geradas.append(tab_id)
                    _msg(f"✅ {info['nome']} gerada com sucesso.")
                else:
                    _msg(f"⚠️ {info['nome']} retornou False.")
            except Exception as exc:
                _msg(f"❌ Erro ao gerar {info['nome']}: {exc}")

        return geradas


def _importar_funcao_tabela(nome_modulo: str, nome_funcao: str) -> Callable:
    """Importa dinamicamente uma função de geração de tabela."""
    import importlib

    # Os módulos estão em c:\funcoes\funcoes_tabelas\tabela_produtos
    if str(TABELA_PRODUTOS_DIR) not in sys.path:
        sys.path.insert(0, str(TABELA_PRODUTOS_DIR))

    modulo = importlib.import_module(nome_modulo)
    # Correção: tabela_codigos usa nome diferente
    if nome_funcao == "gerar_tabela_codigos":
        nome_funcao = "tabela_codigos_mais_descricoes"
    return getattr(modulo, nome_funcao)


class ServicoPipelineCompleto:
    """Orquestra extração Oracle + geração de tabelas."""

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
        """Executa pipeline completo: extração + tabelas."""
        cnpj = ServicoExtracao.sanitizar_cnpj(cnpj)
        resultado = ResultadoPipeline(ok=True, cnpj=cnpj)

        def _msg(texto: str):
            resultado.mensagens.append(texto)
            if progresso:
                progresso(texto)

        # Fase 1: Extração Oracle
        if consultas:
            _msg(f"═══ Fase 1: Extração Oracle ({len(consultas)} consultas) ═══")
            try:
                arquivos = self.servico_extracao.executar_consultas(
                    cnpj, consultas, data_limite, _msg
                )
                resultado.arquivos_gerados.extend(arquivos)
            except Exception as exc:
                resultado.erros.append(f"Falha na extração: {exc}")
                resultado.ok = False
                return resultado

        # Fase 2: Geração de tabelas
        if tabelas:
            _msg(f"═══ Fase 2: Geração de tabelas ({len(tabelas)} selecionadas) ═══")
            try:
                geradas = self.servico_tabelas.gerar_tabelas(cnpj, tabelas, _msg)
                resultado.arquivos_gerados.extend(geradas)
            except Exception as exc:
                resultado.erros.append(f"Falha na geração de tabelas: {exc}")
                resultado.ok = False

        if resultado.ok:
            _msg(f"═══ Pipeline concluído para CNPJ {cnpj} ═══")
        return resultado
