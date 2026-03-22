"""
Serviço de agregação de produtos.
Refatorado para nomes em Português e integração com c:\\funcoes.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from src.config import AGGREGATION_LOG_FILE, CNPJ_ROOT
from src.utilitarios.text import natural_sort_key, normalize_text

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
    def _escolher_moda(valores: list[Any]) -> str | None:
        if not valores: return None
        limpos = [str(v).strip() for v in valores if v not in (None, "", []) and str(v).strip()]
        if not limpos:
            return None
        counts = Counter(limpos)
        max_count = max(counts.values())
        candidatos = [valor for valor, count in counts.items() if count == max_count]
        return sorted(candidatos, key=natural_sort_key)[0]

    @staticmethod
    def caminho_tabela_editavel(cnpj: str) -> Path:
        """Caminho para a tabela final editável do CNPJ."""
        return CNPJ_ROOT / cnpj / "analises" / "produtos" / f"tabela_produtos_editavel_{cnpj}.parquet"

    @staticmethod
    def caminho_tabela_origem(cnpj: str) -> Path:
        """Caminho para a tabela de descrições gerada pelo pipeline."""
        return CNPJ_ROOT / cnpj / "analises" / "produtos" / f"tabela_descricoes_{cnpj}.parquet"

    def carregar_tabela_editavel(self, cnpj: str) -> Path:
        """Copia a tabela de descrições para a tabela editável se esta não existir, ou atualiza o schema."""
        origem = self.caminho_tabela_origem(cnpj)
        destino = self.caminho_tabela_editavel(cnpj)

        if not origem.exists():
            if destino.exists():
                 return destino
            raise FileNotFoundError(f"Tabela de descrições não encontrada em: {origem}")

        # Se o destino já existe, verificamos se precisa de migração de nomes (ex: desc_norm -> descricao_normalizada)
        if destino.exists():
             try:
                 df_check = pl.read_parquet(destino)
                 mudou = False
                 if "desc_norm" in df_check.columns and "descricao_normalizada" not in df_check.columns:
                     df_check = df_check.rename({"desc_norm": "descricao_normalizada"})
                     mudou = True
                 
                 # Migração de lista_chave_item_individualizado -> lista_itens_id
                 if "lista_chave_item_individualizado" in df_check.columns:
                     df_check = df_check.rename({"lista_chave_item_individualizado": "lista_itens_id"})
                     mudou = True
                     
                 if mudou:
                     df_check.write_parquet(destino)
                     return destino
                 
                 if "descricao_normalizada" in df_check.columns:
                     return destino
             except:
                 pass

        # Cria/Sobrescreve se for novo ou se a migração falhou/não tinha destino
        df = pl.read_parquet(origem)
        
        # Migração de nomes na origem também (se necessário)
        renomes = {}
        if "desc_norm" in df.columns: renomes["desc_norm"] = "descricao_normalizada"
        if "lista_chave_item_individualizado" in df.columns: renomes["lista_chave_item_individualizado"] = "lista_itens_id"
        
        if renomes:
            df = df.rename(renomes)

        # Garante que as colunas básicas existam
        if "verificado" not in df.columns:
            df = df.with_columns(pl.lit(False).alias("verificado"))
        
        # Implementa chave_id e lista_chave_produto se não existirem
        if "chave_id" not in df.columns:
            df = df.with_columns(pl.col("chave_produto").alias("chave_id"))
        
        if "lista_chave_produto" not in df.columns:
            # Transforma a chave_produto atual em uma lista unitária
            df = df.with_columns(
                pl.col("chave_produto").map_elements(lambda x: [x], return_dtype=pl.List(pl.String), skip_nulls=False).alias("lista_chave_produto")
            )
            
        if "lista_unidades" in df.columns:
            df = df.drop("lista_unidades")
            
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

        # Unir lista de itens (chave_item_id)
        todas_chaves_itens = []
        for r in linhas:
            todas_chaves_itens.extend(self._garantir_lista(r.get("lista_itens_id")))
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

        # Chaves de produto originais
        lista_chv_produtos = self._mesclar_colunas_lista(linhas, "chave_produto")
        if not lista_chv_produtos:
            lista_chv_produtos = [hashlib.md5(str(r).encode()).hexdigest() for r in linhas]

        # Gerar chave_id única (MD5 das chaves originais ordenadas)
        texto_chave_id = "".join(sorted(lista_chv_produtos))
        chave_id = hashlib.md5(texto_chave_id.encode()).hexdigest()

        lista_sefin = self._mesclar_colunas_lista(linhas, "lista_co_sefin_inferido")

        agregada = {
            "chave_id": chave_id,
            "lista_chave_produto": lista_chv_produtos,
            "chave_produto": chave_id, 
            "descricao": desc,
            "descricao_normalizada": desc_norm,
            "lista_itens_id": lista_final_itens,
            "lista_codigos": mesclados_codigos,
            "codigo_padrao": codigo_padrao,
            "descricao_padrao": desc_padrao,
            "ncm_padrao": _get_moda_lista("lista_ncm") or _get_moda_lista("ncm_padrao"),
            "cest_padrao": _get_moda_lista("lista_cest") or _get_moda_lista("cest_padrao"),
            "gtin_padrao": _get_moda_lista("lista_gtin") or _get_moda_lista("gtin_padrao"),
            "tipo_item_padrao": _get_moda_lista("lista_tipo_item") or _get_moda_lista("tipo_item_padrao"),
            "unid_padrao": _get_moda_lista("lista_unids") or _get_moda_lista("unid_padrao"),
            "co_sefin_agr": self._escolher_moda(lista_sefin),
            "co_sefin_agr_divergente": len(set([str(s) for s in lista_sefin if s])) > 1,
            "lista_tipo_item": self._mesclar_colunas_lista(linhas, "lista_tipo_item"),
            "lista_ncm": self._mesclar_colunas_lista(linhas, "lista_ncm"),
            "lista_cest": self._mesclar_colunas_lista(linhas, "lista_cest"),
            "lista_gtin": self._mesclar_colunas_lista(linhas, "lista_gtin"),
            "lista_unids": self._mesclar_colunas_lista(linhas, "lista_unids"),
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

        # Recalcular totais após agregação (para a tabela inteira, garantindo integridade)
        self.recalcular_valores_totais(cnpj)

        # Buscar linha atualizada do item agregado para o log (com os totais novos)
        df_final = pl.read_parquet(destino)
        agregada_com_totais = df_final.filter(pl.col("chave_produto") == linha_agregada["chave_produto"]).to_dicts()[0]

        self._registrar_log(cnpj=cnpj, originais=linhas_selecionadas, agregada=agregada_com_totais)
        return ResultadoAgregacao(caminho_destino=destino, linha_agregada=agregada_com_totais, chaves_removidas=chaves_removidas)

    def _registrar_log(self, cnpj: str, originais: list[dict[str, Any]], agregada: dict[str, Any]) -> None:
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "cnpj": cnpj,
            "chave_produto": agregada["chave_produto"],
            "descricao": agregada["descricao"],
            "unidade": agregada.get("unid_padrao"),
            "ncm": agregada.get("ncm_padrao"),
            "tot_v_entradas": agregada.get("tot_v_entradas"),
            "tot_v_saidas": agregada.get("tot_v_saidas"),
            "itens_unificados": len(originais)
        }
        with self.arquivo_log.open("a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

    def recalcular_todos_padroes(self, cnpj: str) -> bool:
        """Recalcula campos padrão (NCM, CEST, GTIN, etc)."""
        destino = self.caminho_tabela_editavel(cnpj)
        arq_itens = CNPJ_ROOT / cnpj / "analises" / "produtos" / f"tabela_itens_caracteristicas_{cnpj}.parquet"
        
        if not destino.exists() or not arq_itens.exists():
             return False

        df_edit = pl.read_parquet(destino)
        df_itens = pl.read_parquet(arq_itens)
        
        # CORREÇÃO: Usar os nomes de colunas que realmente existem em df_itens
        # Transformamos lista_unidades em unidade pegando o primeiro item para manter a lógica original
        if "lista_unidades" in df_itens.columns and "unidade" not in df_itens.columns:
            df_itens = df_itens.with_columns(
                pl.col("lista_unidades").list.first().alias("unidade")
            )

        campos_recalc = ["ncm", "cest", "gtin", "tipo_item", "unidade", "co_sefin_inferido"]
        colunas_disponiveis = [c for c in campos_recalc if c in df_itens.columns]
        
        # CORREÇÃO: Usar "chave_item_individualizado" em vez de "chave_item_id"
        item_map = df_itens.select(["chave_item_individualizado"] + colunas_disponiveis)

        df_exploded = df_edit.select(["chave_produto", "lista_itens_id"]).explode("lista_itens_id")
        
        # CORREÇÃO: O join deve ser feito com "chave_item_individualizado"
        df_join = df_exploded.join(
            item_map, 
            left_on="lista_itens_id", 
            right_on="chave_item_individualizado", 
            how="left"
        )
        
        def _get_moda(s: pl.Series) -> str | None:
            return self._escolher_moda(s.to_list())

        mapping_padrao = {
            "ncm": "ncm_padrao", "cest": "cest_padrao", "gtin": "gtin_padrao",
            "tipo_item": "tipo_item_padrao", "unidade": "unid_padrao", "co_sefin_inferido": "co_sefin_agr"
        }

        agg_exprs = []
        for orig, dest in mapping_padrao.items():
            if orig in df_join.columns:
                agg_exprs.append(pl.col(orig).map_elements(_get_moda, return_dtype=pl.String).alias(dest))

        df_recalc = df_join.group_by("chave_produto").agg(agg_exprs)
        
        df_sem_padroes = df_edit.drop([c for c in mapping_padrao.values() if c in df_edit.columns])
        df_novo = df_sem_padroes.join(df_recalc, on="chave_produto", how="left")
        
        df_novo.write_parquet(destino, compression="snappy")
        return True

    def recalcular_valores_totais(self, cnpj: str) -> bool:
        """Calcula totais baseado nos arquivos brutos."""
        destino = self.caminho_tabela_editavel(cnpj)
        if not destino.exists():
            return False

        arq_itens = CNPJ_ROOT / cnpj / "analises" / "produtos" / f"tabela_itens_caracteristicas_{cnpj}.parquet"
        if not arq_itens.exists():
            return False

        df_itens = pl.read_parquet(arq_itens)
        df_edit = pl.read_parquet(destino)

        # CORREÇÃO: Criar a coluna 'unidade' extraindo o texto de 'lista_unidades' se necessário
        if "lista_unidades" in df_itens.columns and "unidade" not in df_itens.columns:
            df_itens = df_itens.with_columns(
                pl.col("lista_unidades").list.join(", ").alias("unidade")
            )
        
        # Mapeamento Produto -> Itens
        df_map = df_edit.select(["chave_produto", "lista_itens_id"]).explode("lista_itens_id")

        # CORREÇÃO: Usar chave_item_individualizado como chave de join à direita
        df_vols = df_map.join(
            df_itens, 
            left_on="lista_itens_id", 
            right_on="chave_item_individualizado", 
            how="inner"
        )

        def _format_totals(df, val_col, name_col):
             df_sum = (
                 df.group_by(["chave_produto", "unidade"])
                 .agg(pl.col(val_col).sum())
                 .filter(pl.col(val_col) > 0)
                 .sort(["chave_produto", "unidade"])
             )
             
             if df_sum.is_empty():
                 return pl.DataFrame({"chave_produto": df_edit["chave_produto"], name_col: [""] * len(df_edit)})

             return (
                 df_sum
                 .with_columns(
                     ("[" + pl.col("unidade") + "; " + pl.col(val_col).cast(pl.Int64).cast(pl.String) + "]").alias("_str")
                 )
                 .group_by("chave_produto")
                 .agg(pl.col("_str").str.join("; ").alias(name_col))
             )

        df_f_ent = _format_totals(df_vols, "valor_total", "tot_v_entradas")
        
        df_edit = df_edit.drop([c for c in ["tot_v_entradas"] if c in df_edit.columns])
        df_edit = df_edit.join(df_f_ent, on="chave_produto", how="left")
        df_edit = df_edit.with_columns(pl.col("tot_v_entradas").fill_null(""))

        df_edit.write_parquet(destino, compression="snappy")
        return True

    def ler_linhas_log(self, cnpj: str | None = None, limite: int = 200) -> list[dict[str, Any]]:
        if not self.arquivo_log.exists():
            return []
            
        resultados = []
        try:
            with self.arquivo_log.open("r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip(): continue
                    try:
                        data = json.loads(line)
                        if cnpj is None or data.get("cnpj") == cnpj:
                             resultados.append(data)
                    except json.JSONDecodeError: continue
        except Exception: return []
        return resultados[-limite:]

    def _mesclar_colunas_lista(self, linhas: list[dict[str, Any]], coluna: str) -> list[str]:
        valores = []
        for linha in linhas:
            valores.extend(self._garantir_lista(linha.get(coluna)))
        return sorted(set(valores), key=natural_sort_key)
