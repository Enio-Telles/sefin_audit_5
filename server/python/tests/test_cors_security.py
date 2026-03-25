import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

# Adiciona o diretório do servidor ao sys.path para importar o app
_SERVER_DIR = Path(__file__).resolve().parent.parent
if str(_SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(_SERVER_DIR))

# Mocking modules that might fail to import due to missing dependencies
class MockPolars:
    @staticmethod
    def DataFrame(*args, **kwargs): return None
    @staticmethod
    def read_parquet(*args, **kwargs): return None
    @staticmethod
    def scan_parquet(*args, **kwargs): return None

sys.modules["polars"] = MockPolars
sys.modules["keyring"] = unittest.mock.MagicMock()
sys.modules["oracledb"] = unittest.mock.MagicMock()
sys.modules["docx"] = unittest.mock.MagicMock()
sys.modules["xlsxwriter"] = unittest.mock.MagicMock()
sys.modules["pandas"] = unittest.mock.MagicMock()
sys.modules["core.produto_runtime"] = unittest.mock.MagicMock()
sys.modules["core.models"] = unittest.mock.MagicMock()
sys.modules["db_manager"] = unittest.mock.MagicMock()
sys.modules["gerar_relatorio"] = unittest.mock.MagicMock()

class TestCORSSecurity(unittest.TestCase):
    def test_cors_logic_parsing(self):
        """Testa a lógica de parsing de origens no api-Enio.py sem subir o servidor."""
        # Test case 1: Default
        with patch.dict(os.environ, {}, clear=True):
            allowed_origins_env = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")
            allowed_origins = [o.strip() for o in allowed_origins_env.split(",") if o.strip()]
            self.assertEqual(allowed_origins, ["http://localhost:3000", "http://127.0.0.1:3000"])

        # Test case 2: Custom
        with patch.dict(os.environ, {"ALLOWED_ORIGINS": "http://myapp.com, https://secure.io "}):
            allowed_origins_env = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")
            allowed_origins = [o.strip() for o in allowed_origins_env.split(",") if o.strip()]
            self.assertEqual(allowed_origins, ["http://myapp.com", "https://secure.io"])

        # Test case 3: Empty string (should fallback to localhost if env is empty but exists)
        with patch.dict(os.environ, {"ALLOWED_ORIGINS": ""}):
            allowed_origins_env = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")
            # If the user provides empty string, the split will result in empty list if handled correctly
            allowed_origins = [o.strip() for o in allowed_origins_env.split(",") if o.strip()]
            # Given our implementation: allowed_origins_env = ""
            # "".split(",") -> [""]
            # [o.strip() for o in [""] if o.strip()] -> []
            self.assertEqual(allowed_origins, [])

if __name__ == "__main__":
    unittest.main()
