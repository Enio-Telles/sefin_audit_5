import json
from pathlib import Path
from src.config import APP_STATE_ROOT

STATE_FILE = APP_STATE_ROOT / "ui_state.json"

class StateService:
    """Serviço para persistir o estado da interface (ex: últimas seleções)."""
    
    def __init__(self, state_file: Path = STATE_FILE):
        self.state_file = state_file
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

    def load_state(self) -> dict:
        if not self.state_file.exists():
            return {}
        try:
            return json.loads(self.state_file.read_text(encoding="utf-8"))
        except:
            return {}

    def save_state(self, state: dict):
        current = self.load_state()
        current.update(state)
        self.state_file.write_text(json.dumps(current, indent=2, ensure_ascii=False), encoding="utf-8")
