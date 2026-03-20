from __future__ import annotations

from pathlib import Path

from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QVBoxLayout,
)
from PySide6.QtCore import Qt


class ColumnSelectorDialog(QDialog):
    def __init__(self, columns: list[str], visible_columns: list[str], parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Selecionar colunas visíveis")
        self.resize(420, 520)

        self.list_widget = QListWidget()
        visible = set(visible_columns)
        for col in columns:
            item = QListWidgetItem(col)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked if col in visible else Qt.Unchecked)
            self.list_widget.addItem(item)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addWidget(self.list_widget)
        layout.addWidget(buttons)

    def selected_columns(self) -> list[str]:
        selected = []
        for idx in range(self.list_widget.count()):
            item = self.list_widget.item(idx)
            if item.checkState() == Qt.Checked:
                selected.append(item.text())
        return selected


class DialogoSelecaoConsultas(QDialog):
    """Diálogo para selecionar quais consultas SQL executar."""

    def __init__(self, consultas: list[Path], pre_selecionados: list[str] | None = None, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Selecionar consultas SQL para execução")
        self.resize(520, 480)
        self._consultas = consultas
        self._pre = set(pre_selecionados) if pre_selecionados else None

        layout = QVBoxLayout(self)

        layout.addWidget(QLabel("Marque as consultas SQL que deseja executar:"))

        # Selecionar / Desmarcar todos
        self.chk_todos = QCheckBox("Selecionar todas")
        self.chk_todos.setChecked(self._pre is None)
        self.chk_todos.stateChanged.connect(self._alternar_todos)
        layout.addWidget(self.chk_todos)

        self.lista = QListWidget()
        for sql_path in consultas:
            item = QListWidgetItem(sql_path.stem)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            
            # Se houver pré-seleção, usa ela; senão marca tudo por padrão
            esta_marcado = (self._pre is None) or (sql_path.stem in self._pre)
            item.setCheckState(Qt.Checked if esta_marcado else Qt.Unchecked)
            
            item.setToolTip(str(sql_path))
            item.setData(Qt.UserRole, str(sql_path))
            self.lista.addItem(item)
        layout.addWidget(self.lista, 1)

        botoes = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        botoes.accepted.connect(self.accept)
        botoes.rejected.connect(self.reject)
        layout.addWidget(botoes)

    def _alternar_todos(self, estado: int) -> None:
        marcado = Qt.Checked if estado == Qt.Checked.value else Qt.Unchecked
        for idx in range(self.lista.count()):
            self.lista.item(idx).setCheckState(marcado)

    def consultas_selecionadas(self) -> list[Path]:
        """Retorna os caminhos das consultas selecionadas."""
        selecionadas = []
        for idx in range(self.lista.count()):
            item = self.lista.item(idx)
            if item.checkState() == Qt.Checked:
                selecionadas.append(Path(item.data(Qt.UserRole)))
        return selecionadas


class DialogoSelecaoTabelas(QDialog):
    """Diálogo para selecionar quais tabelas gerar."""

    def __init__(self, tabelas: list[dict[str, str]], pre_selecionados: list[str] | None = None, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Selecionar tabelas a gerar")
        self.resize(520, 380)
        self._tabelas = tabelas
        self._pre = set(pre_selecionados) if pre_selecionados else None

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(
            "Marque as tabelas que deseja gerar.\n"
            "A ordem de execução respeita as dependências automaticamente."
        ))

        # Selecionar / Desmarcar todos
        self.chk_todos = QCheckBox("Selecionar todas")
        self.chk_todos.setChecked(self._pre is None)
        self.chk_todos.stateChanged.connect(self._alternar_todos)
        layout.addWidget(self.chk_todos)

        self.lista = QListWidget()
        for tabela in tabelas:
            texto = f"{tabela['nome']}\n   {tabela['descricao']}"
            item = QListWidgetItem(texto)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            
            # Pré-seleção
            esta_marcado = (self._pre is None) or (tabela["id"] in self._pre)
            item.setCheckState(Qt.Checked if esta_marcado else Qt.Unchecked)
            
            item.setData(Qt.UserRole, tabela["id"])
            self.lista.addItem(item)
        layout.addWidget(self.lista, 1)

        botoes = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        botoes.accepted.connect(self.accept)
        botoes.rejected.connect(self.reject)
        layout.addWidget(botoes)

    def _alternar_todos(self, estado: int) -> None:
        marcado = Qt.Checked if estado == Qt.Checked.value else Qt.Unchecked
        for idx in range(self.lista.count()):
            self.lista.item(idx).setCheckState(marcado)

    def tabelas_selecionadas(self) -> list[str]:
        """Retorna os IDs das tabelas selecionadas."""
        selecionadas = []
        for idx in range(self.lista.count()):
            item = self.lista.item(idx)
            if item.checkState() == Qt.Checked:
                selecionadas.append(item.data(Qt.UserRole))
        return selecionadas
