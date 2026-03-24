import importlib.util
import os
from pathlib import Path
from typing import Any, Optional

def get_config_module() -> Any:
    """Carrega o arquivo de configuração raiz dinamicamente."""
    # Como __file__ está em server/python/core/, subimos 4 níveis
    projeto_dir = Path(__file__).resolve().parent.parent.parent.parent
    config_path = projeto_dir / "config.py"
    if not config_path.exists():
        return None
    try:
        spec = importlib.util.spec_from_file_location("sefin_config", str(config_path))
        if spec is None or spec.loader is None:
            return None
        cfg = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cfg)
        return cfg
    except Exception:
        return None

def get_config_var(var_name: str, default: Any = None) -> Any:
    """Obtém uma variável da configuração, retornando o valor default se não existir."""
    cfg = get_config_module()
    if cfg is None:
        return default
    return getattr(cfg, var_name, default)
