1.  **Modify `src/orquestrador.py` to use `pl.scan_parquet` for lazy evaluation**:
    -   Update the `_ler_tab` function to use `pl.scan_parquet` instead of `pl.read_parquet`.
    -   Update the reference tables reading (`sitafe_produto_sefin.parquet` and `fatores_conversao_unidades.parquet`) to use `pl.scan_parquet`.
    -   Handle cases where tables don't exist by returning empty LazyFrames (e.g., `pl.LazyFrame()`).
    -   Modify `executar_processar` to orchestrate lazy dataframes.

2.  **Update `src/transformacao/analise_produtos/itens.py` for LazyFrames**:
    -   Add `@cached_transform` decorator using `src/utilitarios/cache_decorator.py`.
    -   Ensure `_normalizar_schema` and `_gerar_chave_item` work with `LazyFrame`.
    -   Change `pl.DataFrame` to `pl.LazyFrame` in type hints.
    -   Handle empty fragments creation with `pl.LazyFrame({c: [] for c in ...})`.

3.  **Update `src/transformacao/analise_produtos/produtos.py` for LazyFrames**:
    -   Add `@cached_transform` decorator.
    -   Ensure processing logic operates seamlessly on `LazyFrame`.
    -   Change type hints to `pl.LazyFrame`.
    -   Since `produtos` groups by string methods and list aggregations, ensure they are compatible with lazy evaluation.

4.  **Update `src/transformacao/analise_produtos/documentos.py` for LazyFrames**:
    -   Add `@cached_transform` decorator.
    -   Update to accept and return `pl.LazyFrame`.

5.  **Update `src/transformacao/analise_produtos/enriquecimento.py` for LazyFrames**:
    -   Add `@cached_transform` decorator.
    -   Check for emptiness using `lf.columns` since `is_empty()` does not exist on `LazyFrame`.
    -   Optimize the join by casting join keys (if they are string or categorical). Use `pl.Categorical` for join keys as requested. Note: Changing strings to categorical inside the dataframes might require an initial explicit cast or `StringCache()`. We'll just cast keys directly to `String` to be safe if not already, or use `pl.Categorical` if cardinalities are known to be high. The instructions specify "Usar `pl.Categorical` para chaves de alta cardinalidade", so we can cast "ncm" and "chave_item_id" to Categorical before the join.

6.  **Update `src/utilitarios/parquet_utils.py` for Idempotent Saves**:
    -   Modify `salvar_para_parquet` to implement the "DELETE-INSERT" pattern using an atomic save via a temporary file and renaming, with `collect(streaming=True)`.
    -   Save execution metadata as required.

7.  **Run tests and linters**.
    -   Ensure everything is properly formatted and linted.
    -   Run python backend tests to confirm nothing broke.
