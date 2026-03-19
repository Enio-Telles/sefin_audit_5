"""
Serviço de agregação de produtos.
Refatorado para nomes em Português e integração com c:\\funcoes.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from fiscal_app.config import AGGREGATION_LOG_FILE, CNPJ_ROOT
from fiscal_app.utils.text import natural_sort_key, normalize_text

CODE_ENTRY_RE = re.compile(r"\[(.*?);\s*(\d+)\]")


@dataclass
class ResultadoAgregacao:
    """Resultado da agregação de linhas."""
    caminho_destino: Path
    linha_agregada: dict[str, Any]
    chaves_removidas: list[tuple[str, str]]


class ServicoAgregacao:
    """Serviço para gerenciar a agregação manual de produtos."""

    def __init__(self, arquivo_log: Path = AGGREGATION_LOG_FILE):
        self.arquivo_log = arquivo_log
        self.arquivo_log.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def caminho_tabela_editavel(cnpj: str) -> Path:
        """Caminho para a tabela final editável do CNPJ."""
        return CNPJ_ROOT / cnpj / "analises" / "produtos" / f"tabela_produtos_editavel_{cnpj}.parquet"

    @staticmethod
    def caminho_tabela_origem(cnpj: str) -> Path:
        """Caminho para a tabela de descrições gerada pelo pipeline."""
        return CNPJ_ROOT / cnpj / "analises" / "produtos" / f"tabela_descricoes_{cnpj}.parquet"

    def carregar_tabela_editavel(self, cnpj: str) -> Path:
        """Copia a tabela de descrições para a tabela editável se esta não existir."""
        origem = self.caminho_tabela_origem(cnpj)
        destino = self.caminho_tabela_editavel(cnpj)

        if destino.exists():
            return destino

        if not origem.exists():
            raise FileNotFoundError(f"Tabela de descrições não encontrada em: {origem}")

        df = pl.read_parquet(origem)
        # Garante que as colunas básicas existam
        if "verificado" not in df.columns:
            df = df.with_columns(pl.lit(False).alias("verificado"))
            
        df.write_parquet(destino, compression="snappy")
        return destino

    @staticmethod
    def _garantir_lista(valor: Any) -> list[str]:
        if valor is None:
            return []
        if isinstance(valor, list):
            return [str(item) for item in valor if item not in (None, "")]
        text = str(valor).strip()
        if not text:
            return []
        return [text]

    @staticmethod
    def _analisar_codigos(valores_brutos: list[Any]) -> list[tuple[str, int, str]]:
        parsed: list[tuple[str, int, str]] = []
        for raw in valores_brutos:
            for entry in ServicoAgregacao._garantir_lista(raw):
                match = CODE_ENTRY_RE.match(entry)
                if match:
                    codigo = match.group(1).strip()
                    freq = int(match.group(2))
                    parsed.append((codigo, freq, entry))
                else:
                    parsed.append((entry, 1, entry))
        return parsed

    @staticmethod
    def _escolher_moda(valores: list[Any]) -> str | None:
        limpos = [str(v).strip() for v in valores if v not in (None, "", []) and str(v).strip()]
        if not limpos:
            return None
        counts = Counter(limpos)
        max_count = max(counts.values())
        candidatos = [valor for valor, count in counts.items() if count == max_count]
        return sorted(candidatos, key=natural_sort_key)[0]

    @staticmethod
    def _mesclar_colunas_lista(linhas: list[dict[str, Any]], coluna: str) -> list[str]:
        valores = []
        for linha in linhas:
            valores.extend(ServicoAgregacao._garantir_lista(linha.get(coluna)))
        return sorted(set(valores), key=natural_sort_key)

    def construir_linha_agregada(
        self,
        linhas: list[dict[str, Any]],
        descricao_resultante: str | None = None,
        descricao_normalizada_resultante: str | None = None,
    ) -> dict[str, Any]:
        """Cria uma nova linha consolidada a partir de múltiplas linhas selecionadas."""
        if len(linhas) < 2:
            raise ValueError("Selecione pelo menos duas linhas para agregar.")

        # Consolidação de Códigos
        dados_codigos = self._analisar_codigos([r.get("lista_codigos") for r in linhas])
        if not dados_codigos:
            # Fallback para cod_normalizado se lista_codigos estiver vazia
            dados_codigos = self._analisar_codigos([r.get("lista_cod_normalizado") for r in linhas])
            
        freq_codigos: Counter[str] = Counter()
        entradas_originais: dict[str, int] = {}
        for codigo, freq, _ in dados_codigos:
            freq_codigos[codigo] += freq
            entradas_originais[codigo] = max(entradas_originais.get(codigo, 0), freq)

        codigo_padrao = "0"
        if freq_codigos:
            max_freq = max(freq_codigos.values())
            top_codes = [c for c, f in freq_codigos.items() if f == max_freq]
            codigo_padrao = sorted(top_codes, key=natural_sort_key)[0]

        # Descrições
        desc = (descricao_resultante or self._escolher_moda([r.get("descricao") for r in linhas]) or linhas[0].get("descricao") or "").strip()
        desc_norm = (descricao_normalizada_resultante or normalize_text(desc)).strip()

        # Descrição Padrão (a que possui mais palavras)
        desc_padrao = linhas[0].get("descricao_padrao") or linhas[0].get("descricao") or ""
        max_palavras = 0
        for r in linhas:
            dn = str(r.get("descricao") or "")
            count = len(dn.split())
            if count > max_palavras:
                max_palavras = count
                desc_padrao = dn

        # Unir lista de itens (chave_item_individualizado)
        todas_chaves_itens = []
        for r in linhas:
            todas_chaves_itens.extend(self._garantir_lista(r.get("lista_chave_item_individualizado")))
        lista_final_itens = sorted(list(set(todas_chaves_itens)))

        # Gerar chave_produto (MD5 das chaves de itens ordenadas)
        texto_chave = "".join(lista_final_itens)
        chave_produto = hashlib.md5(texto_chave.encode()).hexdigest()

        mesclados_codigos = [f"[{c}; {entradas_originais[c]}]" for c in sorted(entradas_originais, key=natural_sort_key)]

        # Recalcular valores padrão (moda dos itens originais)
        def _get_moda_lista(coluna):
            valores = []
            for r in linhas:
                v = r.get(coluna)
                if v:
                    if isinstance(v, list):
                        valores.extend(v)
                    else:
                        valores.append(v)
            return self._escolher_moda(valores)

        lista_sefin = self._mesclar_colunas_lista(linhas, "lista_co_sefin_inferido")

        agregada = {
            "chave_produto": chave_produto,
            "descricao": desc,
            "lista_chave_item_individualizado": lista_final_itens,
            "lista_codigos": mesclados_codigos,
            "codigo_padrao": codigo_padrao,
            "descricao_padrao": desc_padrao,
            "ncm_padrao": _get_moda_lista("lista_ncm") or _get_moda_lista("ncm_padrao"),
            "cest_padrao": _get_moda_lista("lista_cest") or _get_moda_lista("cest_padrao"),
            "gtin_padrao": _get_moda_lista("lista_gtin") or _get_moda_lista("gtin_padrao"),
            "tipo_item_padrao": _get_moda_lista("lista_tipo_item") or _get_moda_lista("tipo_item_padrao"),
            "unid_padrao": _get_moda_lista("lista_unidades") or _get_moda_lista("lista_unids") or _get_moda_lista("unid_padrao"),
            "co_sefin_agr": self._escolher_moda(lista_sefin),
            "co_sefin_agr_divergente": len(set([str(s) for s in lista_sefin if s])) > 1,
            "lista_tipo_item": self._mesclar_colunas_lista(linhas, "lista_tipo_item"),
            "lista_ncm": self._mesclar_colunas_lista(linhas, "lista_ncm"),
            "lista_cest": self._mesclar_colunas_lista(linhas, "lista_cest"),
            "lista_gtin": self._mesclar_colunas_lista(linhas, "lista_gtin"),
            "lista_unids": self._mesclar_colunas_lista(linhas, "lista_unids"),
            "lista_unidades": self._mesclar_colunas_lista(linhas, "lista_unidades"),
            "lista_co_sefin_inferido": lista_sefin,
            "total_entradas": sum([float(r.get("total_entradas") or 0) for r in linhas]),
            "total_saidas": sum([float(r.get("total_saidas") or 0) for r in linhas]),
            "verificado": True, # Muda para true ao agregar manualmente
        }
        
        # Copiar outros campos que podem existir
        opcionais = ["lista_descr_compl"]
        for op in opcionais:
            if op in linhas[0]:
                agregada[op] = self._mesclar_colunas_lista(linhas, op)

        return agregada

    def agregar_linhas(
        self,
        cnpj: str,
        linhas_selecionadas: list[dict[str, Any]],
        descricao_resultante: str | None = None,
        descricao_normalizada_resultante: str | None = None,
    ) -> ResultadoAgregacao:
        """Executa a agregação e salva na tabela editável."""
        destino = self.carregar_tabela_editavel(cnpj)
        df_atual = pl.read_parquet(destino)
        
        # Identifica chaves únicas das linhas a remover
        # Usamos descricao (ou chave_produto se disponível)
        chaves_removidas = []
        for r in linhas_selecionadas:
            chaves_removidas.append((str(r.get("descricao") or ""), str(r.get("chave_produto") or "")))

        linha_agregada = self.construir_linha_agregada(linhas_selecionadas, descricao_resultante, descricao_normalizada_resultante)

        key_set = set(chaves_removidas)
        linhas_mantidas = []
        for r in df_atual.iter_rows(named=True):
            key = (str(r.get("descricao") or ""), str(r.get("chave_produto") or ""))
            if key not in key_set:
                linhas_mantidas.append(r)

        linhas_atualizadas = linhas_mantidas + [linha_agregada]
        
        # Garante que o esquema seja compatível
        try:
            df_novo = pl.DataFrame(linhas_atualizadas, schema=df_atual.schema)
        except Exception:
            # Fallback se colunas novas apareceram
            df_novo = pl.DataFrame(linhas_atualizadas)
            
        df_novo.write_parquet(destino, compression="snappy")

        self._registrar_log(cnpj=cnpj, destino=destino, originais=linhas_selecionadas, agregada=linha_agregada)
        return ResultadoAgregacao(caminho_destino=destino, linha_agregada=linha_agregada, chaves_removidas=chaves_removidas)

    def _registrar_log(self, cnpj: str, destino: Path, originais: list[dict[str, Any]], agregada: dict[str, Any]) -> None:
        payload = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "cnpj": cnpj,
            "arquivo_destino": str(destino),
            "linhas_origem": [
                {
                    "descricao": r.get("descricao"),
                    "chave_produto": r.get("chave_produto"),
                    "codigo_padrao": r.get("codigo_padrao"),
                }
                for r in originais
            ],
            "resultado": agregada,
        }
        with self.arquivo_log.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def ler_linhas_log(self, limite: int = 200) -> list[str]:
        if not self.arquivo_log.exists():
            return []
        linhas = self.arquivo_log.read_text(encoding="utf-8").splitlines()
        return linhas[-limite:]
