import timeit
from unittest.mock import MagicMock

def current_implementation(df_pd, worksheet, cfg, formatos):
    for col_idx, col_name in enumerate(df_pd.columns):
        worksheet.write(0, col_idx, col_name, formatos["cabecalho"])

    for col_idx, col_name in enumerate(df_pd.columns):
        col_lower = str(col_name).strip().lower()
        dtype_str = str(df_pd[col_name].dtype).lower()
        worksheet.set_column(col_idx, col_idx, 20, "fmt")

def optimized_implementation(df_pd, worksheet, cfg, formatos):
    for col_idx, col_name in enumerate(df_pd.columns):
        col_data = df_pd[col_name]
        col_lower = str(col_name).strip().lower()
        dtype_str = str(col_data.dtype).lower()
        worksheet.write(0, col_idx, col_name, formatos["cabecalho"])
        worksheet.set_column(col_idx, col_idx, 20, "fmt")

# Setup data for benchmark
num_cols = 1000
col_names = [f"Column_{i}" for i in range(num_cols)]

class MockSeries:
    def __init__(self, dtype="object"):
        self.dtype = dtype

class MockDF:
    def __init__(self, columns):
        self.columns = columns
        self._data = {name: MockSeries() for name in columns}
    def __getitem__(self, key):
        return self._data[key]

df_mock = MockDF(col_names)
worksheet_mock = MagicMock()
formatos_mock = {"cabecalho": "fmt_header"}

def run_benchmark():
    n = 1000 # Reduced from 10000
    t_current = timeit.timeit(lambda: current_implementation(df_mock, worksheet_mock, cfg_mock, formatos_mock), number=n)
    t_optimized = timeit.timeit(lambda: optimized_implementation(df_mock, worksheet_mock, cfg_mock, formatos_mock), number=n)

    print(f"Benchmark results ({n} iterations, {num_cols} columns):")
    print(f"Current:   {t_current:.4f}s")
    print(f"Optimized: {t_optimized:.4f}s")
    print(f"Improvement: {(t_current - t_optimized) / t_current * 100:.2f}%")

cfg_mock = {}
if __name__ == "__main__":
    run_benchmark()
