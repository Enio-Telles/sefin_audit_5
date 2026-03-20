"""
Lançador principal do Fiscal Parquet Analyzer refatorado.
"""
import sys
from pathlib import Path

# Adiciona a pasta src ao sys.path para imports absolutos funcionarem
RAIZ_PROJETO = Path(__file__).parent.parent.parent
sys.path.insert(0, str(RAIZ_PROJETO))

from PySide6.QtWidgets import QApplication
from src.interface_grafica.main_window import MainWindow

def main() -> int:
    app = QApplication(sys.argv)
    app.setApplicationName("Fiscal Parquet Analyzer (Refatorado)")
    window = MainWindow()
    window.show()
    return app.exec()

if __name__ == "__main__":
    sys.exit(main())
