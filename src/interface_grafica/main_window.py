
from dataclasses import dataclass
from pathlib import Path

import polars as pl
from PySide6.QtCore import QDate, QThread, Qt, Signal, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QDateEdit,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QComboBox,
    QScrollArea,
    QSplitter,
    QStatusBar,
    QTabWidget,
    QTableView,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from src.config import (
    APP_NAME,
    CNPJ_ROOT,
    CONSULTAS_ROOT,
    DEFAULT_PAGE_SIZE,
)
from src.interface_grafica.modelos.table_model import PolarsTableModel
from src.servicos.aggregation_service import ServicoAgregacao
from src.servicos.export_service import ExportService, ReportConfig
from src.servicos.parquet_service import FilterCondition, ParquetService, PageRequest
from src.servicos.pipeline_funcoes_service import ConfiguracaoPipeline, ResultadoPipeline, ServicoPipelineCompleto
from src.servicos.pipeline_service import PipelineService
from src.servicos.query_worker import QueryWorker
from src.servicos.registry_service import RegistryService
from src.servicos.state_service import StateService
from src.servicos.sql_service import SqlService, ParamInfo, WIDGET_DATE
from src.interface_grafica.dialogs import (
    ColumnSelectorDialog,
    DialogoSelecaoConsultas,
    DialogoSelecaoTabelas,
)
from src.utilitarios.text import remove_accents


class PipelineWorker(QThread):
    finished_ok = Signal(object)
    failed = Signal(str)
    progress = Signal(str)

    def __init__(
        self,
        service: ServicoPipelineCompleto,
        cnpj: str,
        consultas: list[Path],
        tabelas: list[str],
        data_limite: str | None = None,
    ) -> None:
        super().__init__()
        self.service = service
        self.cnpj = cnpj
        self.consultas = consultas
        self.tabelas = tabelas
        self.data_limite = data_limite

    def run(self) -> None:
        try:
            config = ConfiguracaoPipeline(
                cnpj=self.cnpj,
                consultas=self.consultas,
                tabelas=self.tabelas,
                data_limite=self.data_limite,
                progresso=self.progress.emit
            )
            result = self.service.executar_completo(config)
        except Exception as exc:  # pragma: no cover - UI
            self.failed.emit(str(exc))
            return
        
        if result.ok:
            self.finished_ok.emit(result)
        else:
            message = "\n".join(result.erros) if result.erros else "Falha no pipeline."
            self.failed.emit(message or "Falha sem detalhes.")


@dataclass
class ViewState:
    current_cnpj: str | None = None
    current_file: Path | None = None
    current_page: int = 1
    page_size: int = DEFAULT_PAGE_SIZE
    all_columns: list[str] | None = None
    visible_columns: list[str] | None = None
    filters: list[FilterCondition] | None = None
    total_rows: int = 0


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.resize(1560, 920)

        self.registry_service = RegistryService()
        self.state_service = StateService()
        self.parquet_service = ParquetService(root=CNPJ_ROOT)
        self.pipeline_service = PipelineService(output_root=CONSULTAS_ROOT)
        self.servico_pipeline_funcoes = ServicoPipelineCompleto()
        self.export_service = ExportService()
        self.servico_agregacao = ServicoAgregacao()
        self.sql_service = SqlService()

        self.state = ViewState(filters=[])
        self.current_page_df_all = pl.DataFrame()
        self.current_page_df_visible = pl.DataFrame()
        self.table_model = PolarsTableModel()
        self.aggregation_table_model = PolarsTableModel(checkable=True)
        self.results_table_model = PolarsTableModel(checkable=True)
        self.conversion_model = PolarsTableModel()
        self.sql_result_model = PolarsTableModel()
        self.aggregation_basket: list[dict] = []
        self.aggregation_results: list[dict] = []
        self.pipeline_worker: PipelineWorker | None = None
        self.query_worker: QueryWorker | None = None
        self._sql_files: list = []
        self._sql_param_widgets: dict[str, QWidget] = {}
        self._sql_current_sql: str = ""
        self._sql_result_df: pl.DataFrame = pl.DataFrame()

        self._build_ui()
        self._connect_signals()
        self.refresh_cnpjs()
        self.refresh_logs()
        self._populate_sql_combo()

    def _build_ui(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)

        splitter = QSplitter(Qt.Horizontal)
        root_layout.addWidget(splitter)

        splitter.addWidget(self._build_left_panel())
        splitter.addWidget(self._build_right_panel())
        splitter.setSizes([310, 1200])

        self.status = QStatusBar()
        self.setStatusBar(self.status)
        self.status.showMessage("Pronto.")

    def _build_left_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)

        cnpj_box = QGroupBox("CNPJs")
        cnpj_layout = QVBoxLayout(cnpj_box)
        input_line = QHBoxLayout()
        self.cnpj_input = QLineEdit()
        self.cnpj_input.setPlaceholderText("Digite o CNPJ com ou sem máscara")
        self.btn_run_pipeline = QPushButton("Extrair + Processar")
        self.btn_run_pipeline.setStyleSheet("background-color: #2c3e50; color: white; font-weight: bold;")
        input_line.addWidget(self.cnpj_input)
        input_line.addWidget(self.btn_run_pipeline)
        cnpj_layout.addLayout(input_line)

        # Granular actions
        granular_line = QHBoxLayout()
        self.btn_extract_only = QPushButton("Extrair dados")
        self.btn_process_only = QPushButton("Processar dados")
        granular_line.addWidget(self.btn_extract_only)
        granular_line.addWidget(self.btn_process_only)
        cnpj_layout.addLayout(granular_line)

        delete_line = QHBoxLayout()
        self.btn_delete_data = QPushButton("Excluir dados")
        self.btn_delete_cnpj = QPushButton("Excluir CNPJ")
        self.btn_delete_cnpj.setStyleSheet("color: #c0392b;")
        delete_line.addWidget(self.btn_delete_data)
        delete_line.addWidget(self.btn_delete_cnpj)
        cnpj_layout.addLayout(delete_line)

        date_line = QHBoxLayout()
        date_line.addWidget(QLabel("Data limite EFD:"))
        self.date_input = QDateEdit()
        self.date_input.setCalendarPopup(True)
        self.date_input.setDate(QDate.currentDate())
        self.date_input.setDisplayFormat("dd/MM/yyyy")
        date_line.addWidget(self.date_input)
        cnpj_layout.addLayout(date_line)

        actions = QHBoxLayout()
        self.btn_refresh_cnpjs = QPushButton("Atualizar lista")
        self.btn_open_cnpj_folder = QPushButton("Abrir pasta")
        actions.addWidget(self.btn_refresh_cnpjs)
        actions.addWidget(self.btn_open_cnpj_folder)
        cnpj_layout.addLayout(actions)

        self.cnpj_list = QListWidget()
        cnpj_layout.addWidget(self.cnpj_list)
        layout.addWidget(cnpj_box)

        files_box = QGroupBox("Arquivos Parquet do CNPJ")
        files_layout = QVBoxLayout(files_box)
        self.file_tree = QTreeWidget()
        self.file_tree.setHeaderLabels(["Arquivo", "Local"])
        files_layout.addWidget(self.file_tree)
        layout.addWidget(files_box)

        notes = QLabel(
            "Fluxo recomendado: analise um CNPJ, abra a tabela desejada, filtre, selecione colunas e exporte. "
            "Para agregação, trabalhe sobre a tabela desagregada e monte o lote na aba Agregação."
        )
        notes.setWordWrap(True)
        layout.addWidget(notes)
        return panel

    def _build_right_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)

        header = QHBoxLayout()
        self.lbl_context = QLabel("Nenhum arquivo selecionado")
        self.lbl_context.setWordWrap(True)
        header.addWidget(self.lbl_context)
        header.addStretch()
        layout.addLayout(header)

        self.tabs = QTabWidget()
        self.tabs.addTab(self._build_tab_consulta(), "Consulta")
        self.tabs.addTab(self._build_tab_sql_query(), "Consulta SQL")
        self.tabs.addTab(self._build_tab_agregacao(), "Agregação")
        self.tabs.addTab(self._build_tab_conversao(), "Conversão")
        self.tabs.addTab(self._build_tab_logs(), "Logs")
        layout.addWidget(self.tabs)
        return panel

    def _build_tab_consulta(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        filter_box = QGroupBox("Filtros")
        filter_layout = QVBoxLayout(filter_box)
        form = QHBoxLayout()
        self.filter_column = QComboBox()
        self.filter_operator = QComboBox()
        self.filter_operator.addItems(["contém", "igual", "começa com", "termina com", ">", ">=", "<", "<=", "é nulo", "não é nulo"])
        self.filter_value = QLineEdit()
        self.filter_value.setPlaceholderText("Valor do filtro")
        self.btn_add_filter = QPushButton("Adicionar filtro")
        self.btn_clear_filters = QPushButton("Limpar filtros")
        form.addWidget(QLabel("Coluna"))
        form.addWidget(self.filter_column)
        form.addWidget(QLabel("Operador"))
        form.addWidget(self.filter_operator)
        form.addWidget(QLabel("Valor"))
        form.addWidget(self.filter_value)
        form.addWidget(self.btn_add_filter)
        form.addWidget(self.btn_clear_filters)
        filter_layout.addLayout(form)

        self.filter_list = QListWidget()
        self.filter_list.setMaximumHeight(90)
        filter_layout.addWidget(self.filter_list)

        filter_actions = QHBoxLayout()
        self.btn_remove_filter = QPushButton("Remover filtro selecionado")
        self.btn_choose_columns = QPushButton("Selecionar colunas")
        self.btn_prev_page = QPushButton("Página anterior")
        self.btn_next_page = QPushButton("Próxima página")
        self.lbl_page = QLabel("Página 0/0")
        filter_actions.addWidget(self.btn_remove_filter)
        filter_actions.addWidget(self.btn_choose_columns)
        filter_actions.addStretch()
        filter_actions.addWidget(self.btn_prev_page)
        filter_actions.addWidget(self.lbl_page)
        filter_actions.addWidget(self.btn_next_page)
        filter_layout.addLayout(filter_actions)
        layout.addWidget(filter_box)

        export_box = QGroupBox("Exportação")
        export_layout = QHBoxLayout(export_box)
        self.btn_export_excel_full = QPushButton("Excel - tabela completa")
        self.btn_export_excel_filtered = QPushButton("Excel - tabela filtrada")
        self.btn_export_excel_visible = QPushButton("Excel - colunas visíveis")
        self.btn_export_docx = QPushButton("Relatório Word")
        self.btn_export_html_txt = QPushButton("TXT com HTML")
        for btn in [
            self.btn_export_excel_full,
            self.btn_export_excel_filtered,
            self.btn_export_excel_visible,
            self.btn_export_docx,
            self.btn_export_html_txt,
        ]:
            export_layout.addWidget(btn)
        layout.addWidget(export_box)

        quick_filter_layout = QHBoxLayout()
        self.qf_norm = QLineEdit()
        self.qf_norm.setPlaceholderText("Filtrar Desc. Norm")
        self.qf_desc = QLineEdit()
        self.qf_desc.setPlaceholderText("Filtrar Descrição")
        self.qf_ncm = QLineEdit()
        self.qf_ncm.setPlaceholderText("Filtrar NCM")
        self.qf_cest = QLineEdit()
        self.qf_cest.setPlaceholderText("Filtrar CEST")
        
        for w in [self.qf_norm, self.qf_desc, self.qf_ncm, self.qf_cest]:
            w.setMaximumWidth(200)
            quick_filter_layout.addWidget(w)
        quick_filter_layout.addStretch()
        layout.addLayout(quick_filter_layout)

        self.table_view = QTableView()
        self.table_view.setModel(self.table_model)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_view.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setSortingEnabled(False)
        self.table_view.setWordWrap(True)
        self.table_view.verticalHeader().setDefaultSectionSize(60)
        self.table_view.horizontalHeader().setMinimumSectionSize(40)
        self.table_view.horizontalHeader().setDefaultSectionSize(200)
        self.table_view.horizontalHeader().setMaximumSectionSize(300)
        self.table_view.setStyleSheet("QTableView::item { padding: 4px 2px; }")
        self.table_view.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table_view, 1)
        return tab

    def _build_tab_agregacao(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        top_box = QGroupBox("Tabela Editável (Selecione linhas para agregar)")
        top_layout = QVBoxLayout(top_box)
        
        toolbar = QHBoxLayout()
        self.btn_open_editable_table = QPushButton("Abrir Tabela Editável")
        self.btn_execute_aggregation = QPushButton("Agregar Descrições (da seleção)")
        self.btn_recalc_defaults = QPushButton("♻️  Recalcular Padrões (Geral)")
        self.btn_recalc_totals = QPushButton("💰  Recalcular Totais")
        
        toolbar.addWidget(self.btn_open_editable_table)
        toolbar.addWidget(self.btn_execute_aggregation)
        toolbar.addWidget(self.btn_recalc_defaults)
        toolbar.addWidget(self.btn_recalc_totals)
        toolbar.addStretch()
        top_layout.addLayout(toolbar)

        agg_qf_layout = QHBoxLayout()
        self.aqf_norm = QLineEdit()
        self.aqf_norm.setPlaceholderText("Filtrar Desc. Norm")
        self.aqf_desc = QLineEdit()
        self.aqf_desc.setPlaceholderText("Filtrar Descrição")
        self.aqf_ncm = QLineEdit()
        self.aqf_ncm.setPlaceholderText("Filtrar NCM")
        self.aqf_cest = QLineEdit()
        self.aqf_cest.setPlaceholderText("Filtrar CEST")

        for w in [self.aqf_norm, self.aqf_desc, self.aqf_ncm, self.aqf_cest]:
            w.setMaximumWidth(200)
            agg_qf_layout.addWidget(w)
        agg_qf_layout.addStretch()
        top_layout.addLayout(agg_qf_layout)

        self.aggregation_table_view = QTableView()
        self.aggregation_table_view.setModel(self.aggregation_table_model)
        self.aggregation_table_view.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.aggregation_table_view.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.aggregation_table_view.setAlternatingRowColors(True)
        self.aggregation_table_view.setWordWrap(True)
        self.aggregation_table_view.verticalHeader().setDefaultSectionSize(60)
        self.aggregation_table_view.horizontalHeader().setMinimumSectionSize(40)
        self.aggregation_table_view.horizontalHeader().setDefaultSectionSize(200)
        self.aggregation_table_view.horizontalHeader().setMaximumSectionSize(300)
        self.aggregation_table_view.setStyleSheet("QTableView::item { padding: 4px 2px; }")
        top_layout.addWidget(self.aggregation_table_view, 1)
        layout.addWidget(top_box, 3)

        bottom_box = QGroupBox("Resultados da Sessão (Historico)")
        bottom_layout = QVBoxLayout(bottom_box)
        self.results_table_view = QTableView()
        self.results_table_view.setModel(self.results_table_model)
        self.results_table_view.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.results_table_view.setAlternatingRowColors(True)
        self.results_table_view.setWordWrap(True)
        self.results_table_view.verticalHeader().setDefaultSectionSize(60)
        self.results_table_view.horizontalHeader().setMinimumSectionSize(40)
        self.results_table_view.horizontalHeader().setDefaultSectionSize(200)
        self.results_table_view.horizontalHeader().setMaximumSectionSize(300)
        self.results_table_view.setStyleSheet("QTableView::item { padding: 4px 2px; }")
        bottom_layout.addWidget(self.results_table_view, 1)
        layout.addWidget(bottom_box, 1)

        return tab

    # ------------------------------------------------------------------
    # Aba Consulta SQL
    # ------------------------------------------------------------------
    def _build_tab_sql_query(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # --- Linha superior: seletor de SQL + botões ---
        top_bar = QHBoxLayout()
        top_bar.addWidget(QLabel("SQL:"))
        self.sql_combo = QComboBox()
        self.sql_combo.setMinimumWidth(300)
        top_bar.addWidget(self.sql_combo, 1)
        self.btn_sql_execute = QPushButton("▶  Executar Consulta")
        self.btn_sql_execute.setStyleSheet("QPushButton { font-weight: bold; padding: 6px 16px; }")
        self.btn_sql_export = QPushButton("Exportar Excel")
        top_bar.addWidget(self.btn_sql_execute)
        top_bar.addWidget(self.btn_sql_export)
        layout.addLayout(top_bar)

        # --- Splitter: SQL + parâmetros (esquerda) | resultados (direita) ---
        splitter = QSplitter(Qt.Vertical)

        # Parte superior: SQL + parâmetros
        upper_widget = QWidget()
        upper_layout = QHBoxLayout(upper_widget)
        upper_layout.setContentsMargins(0, 0, 0, 0)

        # Visualizador SQL
        sql_group = QGroupBox("Texto SQL")
        sql_group_layout = QVBoxLayout(sql_group)
        self.sql_text_view = QPlainTextEdit()
        self.sql_text_view.setReadOnly(True)
        self.sql_text_view.setStyleSheet(
            "QPlainTextEdit { font-family: 'Consolas', 'Courier New', monospace; "
            "font-size: 12px; background: #1e1e2e; color: #cdd6f4; "
            "border: 1px solid #45475a; border-radius: 4px; padding: 8px; }"
        )
        self.sql_text_view.setMinimumHeight(120)
        sql_group_layout.addWidget(self.sql_text_view)
        upper_layout.addWidget(sql_group, 3)

        # Painel de parâmetros
        param_group = QGroupBox("Parâmetros")
        param_outer_layout = QVBoxLayout(param_group)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self.sql_param_container = QWidget()
        self.sql_param_form = QFormLayout(self.sql_param_container)
        self.sql_param_form.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        scroll.setWidget(self.sql_param_container)
        param_outer_layout.addWidget(scroll)
        upper_layout.addWidget(param_group, 1)

        splitter.addWidget(upper_widget)

        # Parte inferior: resultados
        result_widget = QWidget()
        result_layout = QVBoxLayout(result_widget)
        result_layout.setContentsMargins(0, 0, 0, 0)

        # Status
        self.sql_status_label = QLabel("Selecione um SQL e clique em Executar.")
        self.sql_status_label.setStyleSheet(
            "QLabel { padding: 4px 8px; background: #f0f4ff; border-radius: 4px; "
            "border: 1px solid #d0d8e8; color: #334155; font-weight: bold; }"
        )
        result_layout.addWidget(self.sql_status_label)

        # Filtro rápido nos resultados
        sql_filter_bar = QHBoxLayout()
        self.sql_result_search = QLineEdit()
        self.sql_result_search.setPlaceholderText("Buscar nos resultados...")
        sql_filter_bar.addWidget(self.sql_result_search)
        self.sql_result_page_label = QLabel("")
        self.btn_sql_prev = QPushButton("◀ Anterior")
        self.btn_sql_next = QPushButton("Próxima ▶")
        sql_filter_bar.addWidget(self.btn_sql_prev)
        sql_filter_bar.addWidget(self.sql_result_page_label)
        sql_filter_bar.addWidget(self.btn_sql_next)
        result_layout.addLayout(sql_filter_bar)

        # Tabela de resultados
        self.sql_result_table = QTableView()
        self.sql_result_table.setModel(self.sql_result_model)
        self.sql_result_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.sql_result_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.sql_result_table.setAlternatingRowColors(True)
        self.sql_result_table.setSortingEnabled(False)
        self.sql_result_table.setWordWrap(True)
        self.sql_result_table.verticalHeader().setDefaultSectionSize(60)
        self.sql_result_table.horizontalHeader().setMinimumSectionSize(40)
        self.sql_result_table.horizontalHeader().setDefaultSectionSize(200)
        self.sql_result_table.horizontalHeader().setMaximumSectionSize(400)
        self.sql_result_table.horizontalHeader().setStretchLastSection(True)
        self.sql_result_table.setStyleSheet("QTableView::item { padding: 4px 2px; }")
        result_layout.addWidget(self.sql_result_table, 1)

        splitter.addWidget(result_widget)
        splitter.setSizes([280, 500])

        layout.addWidget(splitter, 1)
        return tab

    def _build_tab_conversao(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        toolbar = QHBoxLayout()
        self.btn_refresh_conversao = QPushButton("Recarregar")
        self.btn_refresh_conversao.setIcon(QApplication.style().standardIcon(QApplication.style().StandardPixmap.SP_BrowserReload))
        self.btn_export_conversao = QPushButton("Exportar Excel")
        self.btn_import_conversao = QPushButton("Importar Excel")
        
        toolbar.addWidget(self.btn_refresh_conversao)
        toolbar.addStretch()
        toolbar.addWidget(self.btn_export_conversao)
        toolbar.addWidget(self.btn_import_conversao)
        layout.addLayout(toolbar)

        self.conversion_table = QTableView()
        self.conversion_table.setModel(self.conversion_model)
        self.conversion_table.setAlternatingRowColors(True)
        self.conversion_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.conversion_table.setSortingEnabled(True)
        layout.addWidget(self.conversion_table)

        return tab

    def _build_tab_logs(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        layout.addWidget(self.log_view)
        return tab

    def _connect_signals(self) -> None:
        self.btn_refresh_cnpjs.clicked.connect(self.refresh_cnpjs)
        self.btn_open_cnpj_folder.clicked.connect(self.open_cnpj_folder)
        self.btn_extract_only.clicked.connect(self.run_extraction_only)
        self.btn_process_only.clicked.connect(self.run_processing_only)
        self.btn_delete_data.clicked.connect(self.delete_processed_data)
        self.btn_delete_cnpj.clicked.connect(self.delete_cnpj_full)
        self.btn_run_pipeline.clicked.connect(self.run_pipeline_for_input)
        
        self.cnpj_list.itemSelectionChanged.connect(self.on_cnpj_selected)
        self.file_tree.itemClicked.connect(self.on_file_activated)
        self.file_tree.itemDoubleClicked.connect(self.on_file_activated)

        self.btn_add_filter.clicked.connect(self.add_filter_from_form)
        self.btn_clear_filters.clicked.connect(self.clear_filters)
        self.btn_remove_filter.clicked.connect(self.remove_selected_filter)
        self.btn_choose_columns.clicked.connect(self.choose_columns)
        self.btn_prev_page.clicked.connect(self.prev_page)
        self.btn_next_page.clicked.connect(self.next_page)

        self.btn_export_excel_full.clicked.connect(lambda: self.export_excel("full"))
        self.btn_export_excel_filtered.clicked.connect(lambda: self.export_excel("filtered"))
        self.btn_export_excel_visible.clicked.connect(lambda: self.export_excel("visible"))
        self.btn_export_docx.clicked.connect(self.export_docx)
        self.btn_export_html_txt.clicked.connect(self.export_txt_html)

        self.btn_open_editable_table.clicked.connect(self.open_editable_aggregation_table)
        self.btn_execute_aggregation.clicked.connect(self.execute_aggregation)
        self.btn_recalc_defaults.clicked.connect(self.recalcular_padroes_agregacao)
        self.btn_recalc_totals.clicked.connect(self.recalcular_totais_agregacao)

        for qf in [self.qf_norm, self.qf_desc, self.qf_ncm, self.qf_cest,
                   self.aqf_norm, self.aqf_desc, self.aqf_ncm, self.aqf_cest]:
            qf.returnPressed.connect(self.apply_quick_filters)

        # --- Consulta SQL tab ---
        self.sql_combo.currentIndexChanged.connect(self._on_sql_selected)
        self.btn_sql_execute.clicked.connect(self._execute_sql_query)
        self.btn_sql_export.clicked.connect(self._export_sql_results)
        self.sql_result_search.returnPressed.connect(self._filter_sql_results)
        self.btn_sql_prev.clicked.connect(self._sql_prev_page)
        self.btn_sql_next.clicked.connect(self._sql_next_page)

        # --- Conversão tab ---
        self.btn_refresh_conversao.clicked.connect(self.atualizar_aba_conversao)
        self.btn_export_conversao.clicked.connect(self.exportar_conversao_excel)
        self.btn_import_conversao.clicked.connect(self.importar_conversao_excel)

    def show_error(self, title: str, message: str) -> None:
        QMessageBox.critical(self, title, message)

    def show_info(self, title: str, message: str) -> None:
        QMessageBox.information(self, title, message)

    def refresh_cnpjs(self) -> None:
        known = {record.cnpj for record in self.registry_service.list_records()}
        known.update(self.parquet_service.list_cnpjs())
        current = self.state.current_cnpj
        self.cnpj_list.clear()
        for cnpj in sorted(known):
            self.cnpj_list.addItem(cnpj)
        if current:
            matches = self.cnpj_list.findItems(current, Qt.MatchExactly)
            if matches:
                self.cnpj_list.setCurrentItem(matches[0])

    def run_pipeline_for_input(self) -> None:
        try:
            cnpj = self.servico_pipeline_funcoes.servico_extracao.sanitizar_cnpj(self.cnpj_input.text())
        except Exception as exc:
            self.show_error("CNPJ inválido", str(exc))
            return

        # 1. Selecionar Consultas SQL
        consultas_disp = self.servico_pipeline_funcoes.servico_extracao.listar_consultas()
        if not consultas_disp:
            self.show_error("Sem consultas", "Nenhum arquivo .sql encontrado em c:\\funcoes\\sql")
            return
            
        ui_state = self.state_service.load_state()
        last_sqls = ui_state.get("last_sqls", [])
            
        dlg_sql = DialogoSelecaoConsultas(consultas_disp, pre_selecionados=last_sqls, parent=self)
        if not dlg_sql.exec():
            return
        sql_selecionados = dlg_sql.consultas_selecionadas()
        
        # Salva seleção de SQLs
        self.state_service.save_state({"last_sqls": [p.stem for p in sql_selecionados]})

        # 2. Selecionar Tabelas
        tabelas_disp = self.servico_pipeline_funcoes.servico_tabelas.listar_tabelas()
        last_tabs = ui_state.get("last_tabelas", [])
        
        dlg_tab = DialogoSelecaoTabelas(tabelas_disp, pre_selecionados=last_tabs, parent=self)
        if not dlg_tab.exec():
            return
        tabelas_selecionadas = dlg_tab.tabelas_selecionadas()
        
        # Salva seleção de tabelas
        self.state_service.save_state({"last_tabelas": tabelas_selecionadas})

        if not sql_selecionados and not tabelas_selecionadas:
            return

        self._set_pipeline_buttons_enabled(False)
        self.status.showMessage(f"Executando pipeline para {cnpj}...")
        
        data_limite = self.date_input.date().toString("dd/MM/yyyy")
        self.pipeline_worker = PipelineWorker(
            self.servico_pipeline_funcoes, 
            cnpj, 
            sql_selecionados, 
            tabelas_selecionadas, 
            data_limite
        )
        self._start_pipeline_worker()

    def _start_pipeline_worker(self) -> None:
        self.pipeline_worker.finished_ok.connect(self.on_pipeline_finished)
        self.pipeline_worker.failed.connect(self.on_pipeline_failed)
        self.pipeline_worker.progress.connect(self.status.showMessage)
        self.pipeline_worker.start()

    def run_extraction_only(self) -> None:
        try:
            cnpj = self.servico_pipeline_funcoes.servico_extracao.sanitizar_cnpj(self.cnpj_input.text())
        except Exception as exc:
            self.show_error("CNPJ inválido", str(exc))
            return

        consultas_disp = self.servico_pipeline_funcoes.servico_extracao.listar_consultas()
        if not consultas_disp:
            return
        
        ui_state = self.state_service.load_state()
        last_sqls = ui_state.get("last_sqls", [])
        dlg_sql = DialogoSelecaoConsultas(consultas_disp, pre_selecionados=last_sqls, parent=self)
        if not dlg_sql.exec():
            return
        sql_selecionados = dlg_sql.consultas_selecionadas()
        self.state_service.save_state({"last_sqls": [p.stem for p in sql_selecionados]})

        if not sql_selecionados:
            return

        self.status.showMessage(f"Extraindo dados para {cnpj}...")
        data_limite = self.date_input.date().toString("dd/MM/yyyy")
        
        self.pipeline_worker = PipelineWorker(
            self.servico_pipeline_funcoes, cnpj, sql_selecionados, [], data_limite
        )
        self._start_pipeline_worker()

    def run_processing_only(self) -> None:
        cnpj = self.state.current_cnpj
        if not cnpj:
            try:
                cnpj = self.servico_pipeline_funcoes.servico_extracao.sanitizar_cnpj(self.cnpj_input.text())
            except Exception:
                self.show_error("Erro", "Selecione um CNPJ ou digite no campo.")
                return

        tabelas_disp = self.servico_pipeline_funcoes.servico_tabelas.listar_tabelas()
        ui_state = self.state_service.load_state()
        last_tabs = ui_state.get("last_tabelas", [])
        dlg_tab = DialogoSelecaoTabelas(tabelas_disp, pre_selecionados=last_tabs, parent=self)
        if not dlg_tab.exec():
            return
        tabelas_selecionadas = dlg_tab.tabelas_selecionadas()
        self.state_service.save_state({"last_tabelas": tabelas_selecionadas})

        if not tabelas_selecionadas:
            return

        self.status.showMessage(f"Processando dados localmente para {cnpj}...")
        self.pipeline_worker = PipelineWorker(
            self.servico_pipeline_funcoes, cnpj, [], tabelas_selecionadas
        )
        self._start_pipeline_worker()

    def delete_processed_data(self) -> None:
        cnpj = self.state.current_cnpj
        if not cnpj:
            return
        
        ans = QMessageBox.question(self, "Confirmar exclusão", f"Deseja excluir apenas as tabelas geradas (pasta analises) do CNPJ {cnpj}?", QMessageBox.Yes | QMessageBox.No)
        if ans == QMessageBox.Yes:
            if self.pipeline_service.delete_processed_data(cnpj):
                self.show_info("Sucesso", "Pasta de análises removida.")
                self.on_cnpj_selected() # Refresh tree
            else:
                self.show_error("Erro", "Não foi possível excluir os dados ou a pasta não existe.")

    def delete_cnpj_full(self) -> None:
        cnpj = self.state.current_cnpj
        if not cnpj:
            return
        
        ans = QMessageBox.warning(self, "PERIGO", f"Deseja excluir TODOS os dados (Extrações + Análises) do CNPJ {cnpj}?\nEsta ação não pode ser desfeita.", QMessageBox.Yes | QMessageBox.No)
        if ans == QMessageBox.Yes:
            if self.pipeline_service.delete_cnpj_all(cnpj):
                self.show_info("Sucesso", f"CNPJ {cnpj} removido do disco.")
                self.state.current_cnpj = None
                self.refresh_cnpjs()
                self.file_tree.clear()
            else:
                self.show_error("Erro", "Falha ao remover diretório do CNPJ.")

    def on_pipeline_finished(self, result: ResultadoPipeline) -> None:
        self._set_pipeline_buttons_enabled(True)
        self.registry_service.upsert(result.cnpj, ran_now=True)
        self.status.showMessage(f"Pipeline concluído para {result.cnpj}.")
        self.refresh_cnpjs()
        matches = self.cnpj_list.findItems(result.cnpj, Qt.MatchExactly)
        if matches:
            self.cnpj_list.setCurrentItem(matches[0])
            self.refresh_file_tree(result.cnpj)
            self.atualizar_aba_conversao()
            
        msg = "\n".join(result.mensagens[-10:]) if result.mensagens else "Processado com sucesso."
        self.show_info("Pipeline concluído", f"CNPJ {result.cnpj} processado.\n\nÚltimas mensagens:\n{msg}")

    def on_pipeline_failed(self, message: str) -> None:
        self._set_pipeline_buttons_enabled(True)
        self.status.showMessage("Falha na execução do pipeline.")
        self.show_error("Falha ao consultar o banco", message)

    def _set_pipeline_buttons_enabled(self, enabled: bool) -> None:
        self.btn_run_pipeline.setEnabled(enabled)
        self.btn_extract_only.setEnabled(enabled)
        self.btn_process_only.setEnabled(enabled)

    def on_cnpj_selected(self) -> None:
        item = self.cnpj_list.currentItem()
        if item is None:
            return
        cnpj = item.text()
        self.state.current_cnpj = cnpj
        self.registry_service.upsert(cnpj, ran_now=False)
        self.refresh_file_tree(cnpj)
        self.atualizar_aba_conversao()
        self.recarregar_historico_agregacao(cnpj)

    def refresh_file_tree(self, cnpj: str) -> None:
        self.file_tree.clear()
        
        root_path = self.parquet_service.cnpj_dir(cnpj)
        
        cat_brutas = QTreeWidgetItem(["Tabelas brutas (SQL)", str(root_path / "arquivos_parquet")])
        cat_analises = QTreeWidgetItem(["Análises de Produtos", str(root_path / "analises" / "produtos")])
        cat_outros = QTreeWidgetItem(["Outros Parquets", str(root_path)])
        
        self.file_tree.addTopLevelItem(cat_brutas)
        self.file_tree.addTopLevelItem(cat_analises)
        self.file_tree.addTopLevelItem(cat_outros)

        first_leaf: QTreeWidgetItem | None = None
        
        for path in self.parquet_service.list_parquet_files(cnpj):
            # Identificar categoria
            if "arquivos_parquet" in str(path.parent):
                parent = cat_brutas
            elif "analises" in str(path.parent) or "produtos" in str(path.parent):
                parent = cat_analises
            else:
                parent = cat_outros
                
            item = QTreeWidgetItem([path.name, str(path.parent)])
            item.setData(0, Qt.UserRole, str(path))
            parent.addChild(item)
            if first_leaf is None:
                first_leaf = item
                
        cat_brutas.setExpanded(True)
        cat_analises.setExpanded(True)
        
        # Limpar categorias vazias
        for cat in [cat_brutas, cat_analises, cat_outros]:
            if cat.childCount() == 0:
                self.file_tree.takeTopLevelItem(self.file_tree.indexOfTopLevelItem(cat))

        if first_leaf is not None:
            self.file_tree.setCurrentItem(first_leaf)
            self.on_file_activated(first_leaf, 0)

    def on_file_activated(self, item: QTreeWidgetItem, _column: int) -> None:
        raw_path = item.data(0, Qt.UserRole)
        if not raw_path:
            return
        self.state.current_file = Path(raw_path)
        self.state.current_page = 1
        self.state.filters = []
        self.current_page_df_all = pl.DataFrame()
        self.current_page_df_visible = pl.DataFrame()
        self.load_current_file(reset_columns=True)
        self.tabs.setCurrentIndex(0)

    def load_current_file(self, reset_columns: bool = False) -> None:
        if self.state.current_file is None:
            return
        try:
            all_columns = self.parquet_service.get_schema(self.state.current_file)
        except Exception as exc:
            self.show_error("Erro ao abrir Parquet", str(exc))
            return
        self.state.all_columns = all_columns
        if reset_columns or not self.state.visible_columns:
            self.state.visible_columns = all_columns[:]
        self.filter_column.clear()
        self.filter_column.addItems(all_columns)
        self.reload_table()

    def reload_table(self, update_main_view: bool = True) -> None:
        if self.state.current_file is None:
            return
        try:
            page_request = PageRequest(
                parquet_path=self.state.current_file,
                conditions=self.state.filters or [],
                visible_columns=self.state.visible_columns or [],
                page=self.state.current_page,
                page_size=self.state.page_size,
            )
            page_result = self.parquet_service.get_page(page_request)
            self.state.total_rows = page_result.total_rows
            self.current_page_df_all = page_result.df_all_columns
            self.current_page_df_visible = page_result.df_visible

            if update_main_view:
                self.table_model.set_dataframe(self.current_page_df_visible)
                self._update_page_label()
                self._update_context_label()
                self._refresh_filter_list_widget()
                self.table_view.resizeColumnsToContents()
        except Exception as exc:
            self.show_error("Erro ao carregar dados", str(exc))

    def _update_page_label(self) -> None:
        total_pages = max(1, ((self.state.total_rows - 1) // self.state.page_size) + 1 if self.state.total_rows else 1)
        if self.state.current_page > total_pages:
            self.state.current_page = total_pages
        self.lbl_page.setText(f"Página {self.state.current_page}/{total_pages} | Linhas filtradas: {self.state.total_rows}")

    def _update_context_label(self) -> None:
        if self.state.current_file is None:
            self.lbl_context.setText("Nenhum arquivo selecionado")
            return
        self.lbl_context.setText(
            f"CNPJ: {self.state.current_cnpj or '-'} | Arquivo: {self.state.current_file.name} | "
            f"Colunas visíveis: {len(self.state.visible_columns or [])}/{len(self.state.all_columns or [])}"
        )

    def add_filter_from_form(self) -> None:
        column = self.filter_column.currentText().strip()
        operator = self.filter_operator.currentText().strip()
        value = self.filter_value.text().strip()
        if not column:
            self.show_error("Filtro inválido", "Selecione uma coluna para filtrar.")
            return
        if operator not in {"é nulo", "não é nulo"} and value == "":
            self.show_error("Filtro inválido", "Informe um valor para o filtro escolhido.")
            return
        self.state.filters = self.state.filters or []
        self.state.filters.append(FilterCondition(column=column, operator=operator, value=value))
        self.state.current_page = 1
        self.filter_value.clear()
        self.reload_table()

    def clear_filters(self) -> None:
        self.state.filters = []
        self.state.current_page = 1
        self.reload_table()

    def remove_selected_filter(self) -> None:
        row = self.filter_list.currentRow()
        if row < 0 or not self.state.filters:
            return
        self.state.filters.pop(row)
        self.state.current_page = 1
        self.reload_table()

    def _refresh_filter_list_widget(self) -> None:
        self.filter_list.clear()
        for cond in self.state.filters or []:
            text = f"{cond.column} {cond.operator} {cond.value}".strip()
            self.filter_list.addItem(text)

    def choose_columns(self) -> None:
        if not self.state.all_columns:
            return
        dialog = ColumnSelectorDialog(self.state.all_columns, self.state.visible_columns or self.state.all_columns, self)
        if dialog.exec():
            selected = dialog.selected_columns()
            if not selected:
                self.show_error("Seleção inválida", "Pelo menos uma coluna deve permanecer visível.")
                return
            self.state.visible_columns = selected
            self.state.current_page = 1
            self.reload_table()

    def prev_page(self) -> None:
        if self.state.current_page > 1:
            self.state.current_page -= 1
            self.reload_table()

    def next_page(self) -> None:
        total_pages = max(1, ((self.state.total_rows - 1) // self.state.page_size) + 1 if self.state.total_rows else 1)
        if self.state.current_page < total_pages:
            self.state.current_page += 1
            self.reload_table()

    def _save_dialog(self, title: str, pattern: str) -> Path | None:
        filename, _ = QFileDialog.getSaveFileName(self, title, str(CONSULTAS_ROOT), pattern)
        return Path(filename) if filename else None

    def _filters_text(self) -> str:
        return " | ".join(f"{f.column} {f.operator} {f.value}".strip() for f in self.state.filters or [])

    def _dataset_for_export(self, mode: str) -> pl.DataFrame:
        if self.state.current_file is None:
            raise ValueError("Nenhum arquivo selecionado.")
        if mode == "full":
            return self.parquet_service.load_dataset(self.state.current_file)
        if mode == "filtered":
            return self.parquet_service.load_dataset(self.state.current_file, self.state.filters or [])
        if mode == "visible":
            return self.parquet_service.load_dataset(
                self.state.current_file,
                self.state.filters or [],
                self.state.visible_columns or [],
            )
        raise ValueError(f"Modo de exportação não suportado: {mode}")

    def export_excel(self, mode: str) -> None:
        try:
            df = self._dataset_for_export(mode)
            target = self._save_dialog("Salvar Excel", "Excel (*.xlsx)")
            if not target:
                return
            self.export_service.export_excel(target, df, sheet_name=self.state.current_file.stem if self.state.current_file else "Dados")
            self.show_info("Exportação concluída", f"Arquivo gerado em:\n{target}")
        except Exception as exc:
            self.show_error("Falha na exportação para Excel", str(exc))

    def export_docx(self) -> None:
        try:
            if self.state.current_file is None:
                raise ValueError("Nenhum arquivo selecionado.")
            df = self.parquet_service.load_dataset(self.state.current_file, self.state.filters or [], self.state.visible_columns or [])
            target = self._save_dialog("Salvar relatório Word", "Word (*.docx)")
            if not target:
                return
            config = ReportConfig(
                title="Relatório Padronizado de Análise Fiscal",
                cnpj=self.state.current_cnpj or "",
                table_name=self.state.current_file.name,
                filters_text=self._filters_text(),
                visible_columns=self.state.visible_columns or [],
            )
            self.export_service.export_docx(target, df, config)
            self.show_info("Relatório gerado", f"Arquivo gerado em:\n{target}")
        except Exception as exc:
            self.show_error("Falha na exportação para Word", str(exc))

    def export_txt_html(self) -> None:
        try:
            if self.state.current_file is None:
                raise ValueError("Nenhum arquivo selecionado.")
            df = self.parquet_service.load_dataset(self.state.current_file, self.state.filters or [], self.state.visible_columns or [])
            config = ReportConfig(
                title="Relatório Padronizado de Análise Fiscal",
                cnpj=self.state.current_cnpj or "",
                table_name=self.state.current_file.name,
                filters_text=self._filters_text(),
                visible_columns=self.state.visible_columns or [],
            )
            html_report = self.export_service.build_html_report(df, config)
            target = self._save_dialog("Salvar TXT com HTML", "TXT (*.txt)")
            if not target:
                return
            self.export_service.export_txt_with_html(target, html_report)
            self.show_info("Relatório HTML/TXT gerado", f"Arquivo gerado em:\n{target}")
        except Exception as exc:
            self.show_error("Falha na exportação TXT/HTML", str(exc))

    def open_editable_aggregation_table(self) -> None:
        if not self.state.current_cnpj:
            self.show_error("CNPJ não selecionado", "Selecione um CNPJ na lista.")
            return
        try:
            target = self.servico_agregacao.carregar_tabela_editavel(self.state.current_cnpj)
            df = pl.read_parquet(target)
            self.state.all_columns = df.columns
            self.aggregation_table_model.set_dataframe(df)
            self.aggregation_table_view.resizeColumnsToContents()
        except Exception as exc:
            self.show_error("Falha ao abrir tabela editável", str(exc))
            return

        self.state.current_file = target
        self.state.current_page = 1
        self.state.filters = []
        self.tabs.setCurrentIndex(2) # Switch to Agregação tab (0-indexed: Consulta, SQL, Agregação, Logs)

    def execute_aggregation(self) -> None:
        if not self.state.current_cnpj:
            self.show_error("CNPJ não selecionado", "Selecione um CNPJ antes de agregar.")
            return

        rows_top = self.aggregation_table_model.get_checked_rows()
        rows_bottom = self.results_table_model.get_checked_rows()
        
        # Merge and de-duplicate
        combined = []
        seen = set()
        for r in (rows_top + rows_bottom):
            # CORREÇÃO: Usar chave_produto para garantir que não vamos fundir/perder itens indevidamente
            key = str(r.get("chave_produto") or "")
            if key not in seen:
                seen.add(key)
                combined.append(r)

        if len(combined) < 2:
            self.show_error("Seleção insuficiente", "Marque pelo menos duas linhas com 'Visto' (pode ser em ambas as tabelas) para agregar.")
            return

        try:
            result = self.servico_agregacao.agregar_linhas(
                cnpj=self.state.current_cnpj,
                linhas_selecionadas=combined,
            )
            # Update the tables to reflect the changes
            self.atualizar_tabelas_agregacao()
            self.recarregar_historico_agregacao(self.state.current_cnpj)
            
            self.show_info(
                "Agregação concluída",
                f"As {len(combined)} descrições foram unificadas em:\n'{result.linha_agregada['descricao']}'"
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.show_error("Erro na agregação", f"Ocorreu um erro ao agregar: {e}")
            
            # Clear checks and reload top table
            self.aggregation_table_model.clear_checked()
            self.results_table_model.clear_checked()
            self.open_editable_aggregation_table()

    def apply_quick_filters(self) -> None:
        idx = self.tabs.currentIndex()
        if idx == 0: # Consulta
            fields = {
                "descricao_normalizada": self.qf_norm.text().strip(),
                "descricao": self.qf_desc.text().strip(),
                "ncm_padrao": self.qf_ncm.text().strip(),
                "cest_padrao": self.qf_cest.text().strip(),
            }
        elif idx == 2: # Agregação
            fields = {
                "descricao_normalizada": self.aqf_norm.text().strip(),
                "descricao": self.aqf_desc.text().strip(), # CORREÇÃO: Nome correto da coluna
                "ncm_padrao": self.aqf_ncm.text().strip(),
                "cest_padrao": self.aqf_cest.text().strip(),
            }
        else:
            return

        # Keep non-quick filters if any, but replace quick filter columns
        quick_cols = set(fields.keys())
        new_filters = [f for f in (self.state.filters or []) if f.column not in quick_cols]
        
        for col, val in fields.items():
            if val:
                # Need to be flexible with column names as they might differ across files
                # We'll use the one present in the schema
                actual_col = col
                if self.state.all_columns:
                    # Match case-insensitive if needed, or handle variations like NCM_padrao
                    alternatives = {
                        "ncm_padrao": ["ncm_padrao", "NCM_padrao", "lista_ncm"],
                        "cest_padrao": ["cest_padrao", "CEST_padrao", "lista_cest"],
                        "descricao_normalizada": ["descricao_normalizada", "descricao", "descr_norm"],
                        "descricao": ["descricao_referencia", "descricao", "lista_descricoes", "descr"],
                        "descricao_referencia": ["descricao_referencia", "descricao"],
                    }
                    if col in alternatives:
                        for alt in alternatives[col]:
                            if alt in self.state.all_columns:
                                actual_col = alt
                                break
                    elif col not in self.state.all_columns:
                        # try case-insensitive and accent-insensitive match
                        target_clean = remove_accents(col).lower()
                        for c in self.state.all_columns:
                            if remove_accents(c).lower() == target_clean:
                                actual_col = c
                                break

                new_filters.append(FilterCondition(column=actual_col, operator="contém", value=val))
        
        self.state.filters = new_filters
        self.state.current_page = 1
        
        self.reload_table(update_main_view=(idx==0))
        if idx == 2:
            # Update aggregation table with the filtered results
            self.aggregation_table_model.set_dataframe(self.current_page_df_all)
            self.aggregation_table_view.resizeColumnsToContents()

    def refresh_logs(self) -> None:
        import json
        logs = [json.dumps(log) for log in self.servico_agregacao.ler_linhas_log()]
        self.log_view.setPlainText("\n".join(logs))

    def open_cnpj_folder(self) -> None:
        if not self.state.current_cnpj:
            self.show_error("CNPJ não selecionado", "Selecione um CNPJ para abrir a pasta.")
            return
        target = self.parquet_service.cnpj_dir(self.state.current_cnpj)
        if not target.exists():
            self.show_error("Pasta inexistente", f"A pasta {target} ainda não foi criada.")
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(target)))

    def atualizar_aba_conversao(self) -> None:
        """Carrega os fatores de conversão do CNPJ atual."""
        cnpj = self.state.current_cnpj
        if not cnpj:
            return

        pasta_produtos = CNPJ_ROOT / cnpj / "analises" / "produtos"
        arq_conversao = pasta_produtos / f"fator_conversao_{cnpj}.parquet"

        if not arq_conversao.exists():
            self.conversion_model.set_dataframe(pl.DataFrame())
            return

        try:
            df = pl.read_parquet(arq_conversao)
            self.conversion_model.set_dataframe(df)
            self.conversion_table.resizeColumnsToContents()
        except Exception as e:
            QMessageBox.warning(self, "Erro", f"Erro ao carregar fatores de conversão: {e}")

    def exportar_conversao_excel(self) -> None:
        """Exporta os fatores de conversão para Excel para edição."""
        df = self.conversion_model.dataframe
        if df.is_empty():
            QMessageBox.information(self, "Aviso", "Não há dados para exportar.")
            return

        path, _ = QFileDialog.getSaveFileName(self, "Salvar Excel", f"fator_conversao_{self.state.current_cnpj}.xlsx", "Excel (*.xlsx)")
        if not path:
            return

        try:
            df.write_excel(path)
            QMessageBox.information(self, "Sucesso", f"Arquivo salvo com sucesso:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao exportar: {e}")

    def importar_conversao_excel(self) -> None:
        """Importa fatores de conversão do Excel, sobrescrevendo o Parquet."""
        cnpj = self.state.current_cnpj
        if not cnpj:
            return

        path, _ = QFileDialog.getOpenFileName(self, "Abrir Excel", "", "Excel (*.xlsx)")
        if not path:
            return

        try:
            df_excel = pl.read_excel(path)
            # Validação conforme documentação: ano, codigo_produto_ajustado, unid, fator
            mapping = {
                "ano": "ano",
                "codigo_produto_ajustado": "chave_produto",
                "unid": "unidade",
                "fator": "fator_de_conversao"
            }
            cols_obrigatorias = list(mapping.keys())
            if not all(c in df_excel.columns for c in cols_obrigatorias):
                raise ValueError(f"O Excel deve conter as colunas: {cols_obrigatorias}")

            pasta_produtos = CNPJ_ROOT / cnpj / "analises" / "produtos"
            nome_saida = f"fator_conversao_{cnpj}.parquet"
            
            # Renomeia para colunas internas e garante tipos
            df_imp = df_excel.select(cols_obrigatorias).rename({c: mapping[c] for c in cols_obrigatorias})
            df_imp = df_imp.with_columns([
                pl.col("fator_de_conversao").cast(pl.Float64)
            ])

            df_imp.write_parquet(pasta_produtos / nome_saida)
            self.atualizar_aba_conversao()
            QMessageBox.information(self, "Sucesso", "Fatores de conversão importados com sucesso.")
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao importar: {e}")

    def recalcular_padroes_agregacao(self) -> None:
        """Invoca o serviço para recalcular todos os padrões do CNPJ atual."""
        cnpj = self.state.current_cnpj
        if not cnpj:
            return
        
        ret = QMessageBox.question(self, "Recalcular Padrões", 
                                   "Isso irá atualizar NCM, CEST, GTIN, UNID e SEFIN de TODOS os grupos baseando-se na moda dos itens originais.\nProsseguir?",
                                   QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if ret == QMessageBox.StandardButton.No:
            return
        
        try:
            ok = self.servico_agregacao.recalcular_todos_padroes(cnpj)
            if ok:
                self.atualizar_tabelas_agregacao()
                QMessageBox.information(self, "Sucesso", "Valores padrão recalculados com sucesso para toda a tabela.")
            else:
                QMessageBox.warning(self, "Aviso", "Não foi possível recalcular. Verifique se as tabelas existem.")
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao recalcular: {e}")

    def recalcular_totais_agregacao(self) -> None:
        """Invoca o serviço para recalcular totais de entrada/saída do CNPJ atual."""
        cnpj = self.state.current_cnpj
        if not cnpj:
            return
        
        ret = QMessageBox.question(self, "Recalcular Totais", 
                                   "Isso irá calcular os totais de Entrada (C170) e Saída (NFe) para todos os produtos (apenas operações mercantis).\nProsseguir?",
                                   QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if ret == QMessageBox.StandardButton.No:
            return
        
        self.status.showMessage("Calculando totais de valores...")
        try:
            ok = self.servico_agregacao.recalcular_valores_totais(cnpj)
            if ok:
                self.atualizar_tabelas_agregacao()
                QMessageBox.information(self, "Sucesso", "Totais de valores recalculados com sucesso.")
            else:
                QMessageBox.warning(self, "Aviso", "Não foi possível recalcular os totais.")
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao recalcular totais: {e}")
        finally:
            self.status.showMessage("Pronto.")

    def recarregar_historico_agregacao(self, cnpj: str) -> None:
        """Lê o log persistente e preenche o painel de resultados da sessão."""
        historico = self.servico_agregacao.ler_linhas_log(cnpj=cnpj)
        # O model espera uma lista de dicts (linhas da tabela)
        self.results_table_model.set_dataframe(pl.DataFrame(historico))
        self.results_table_view.resizeColumnsToContents()

    def atualizar_tabelas_agregacao(self) -> None:
        """Atualiza os modelos das tabelas de agregação."""
        cnpj = self.state.current_cnpj
        if not cnpj:
            return
        
        path = self.servico_agregacao.caminho_tabela_editavel(cnpj)
        if path.exists():
            df = pl.read_parquet(path)
            self.aggregation_table_model.set_dataframe(df)
            self.aggregation_table_view.resizeColumnsToContents()
            
    # ==================================================================
    # Consulta SQL — métodos de suporte
    # ==================================================================
    _sql_result_page: int = 1
    _sql_result_page_size: int = DEFAULT_PAGE_SIZE

    def _populate_sql_combo(self) -> None:
        """Carrega a lista de arquivos SQL disponíveis no combo."""
        self._sql_files = self.sql_service.list_sql_files()
        self.sql_combo.blockSignals(True)
        self.sql_combo.clear()
        self.sql_combo.addItem("— Selecione uma consulta —")
        for info in self._sql_files:
            self.sql_combo.addItem(f"{info.display_name}  [{info.source_dir}]", str(info.path))
        self.sql_combo.blockSignals(False)

    def _on_sql_selected(self, index: int) -> None:
        """Ao selecionar um SQL no combo: lê, exibe e gera o formulário de parâmetros."""
        if index <= 0:
            self.sql_text_view.setPlainText("")
            self._clear_param_form()
            self._sql_current_sql = ""
            return
        path_str = self.sql_combo.itemData(index)
        if not path_str:
            return
        try:
            sql_text = self.sql_service.read_sql(Path(path_str))
        except Exception as exc:
            self.show_error("Erro ao ler SQL", str(exc))
            return
        self._sql_current_sql = sql_text
        self.sql_text_view.setPlainText(sql_text)
        params = self.sql_service.extract_params(sql_text)
        self._rebuild_param_form(params)

    def _clear_param_form(self) -> None:
        """Remove todos os campos do formulário de parâmetros."""
        while self.sql_param_form.rowCount() > 0:
            self.sql_param_form.removeRow(0)
        self._sql_param_widgets.clear()

    def _rebuild_param_form(self, params: list[ParamInfo]) -> None:
        """Reconstroi o formulário de parâmetros conforme os parâmetros detectados."""
        self._clear_param_form()
        for param in params:
            label = QLabel(f":{param.name}")
            label.setStyleSheet("font-weight: bold; color: #1e40af;")
            if param.widget_type == WIDGET_DATE:
                widget = QDateEdit()
                widget.setCalendarPopup(True)
                widget.setDate(QDate.currentDate())
                widget.setDisplayFormat("dd/MM/yyyy")
            else:
                widget = QLineEdit()
                if param.placeholder:
                    widget.setPlaceholderText(param.placeholder)
                # Pré-preencher CNPJ se disponível
                if "cnpj" in param.name.lower() and self.state.current_cnpj:
                    widget.setText(self.state.current_cnpj)
            self.sql_param_form.addRow(label, widget)
            self._sql_param_widgets[param.name] = widget

    def _collect_param_values(self) -> dict[str, str]:
        """Coleta os valores do formulário de parâmetros."""
        values: dict[str, str] = {}
        for name, widget in self._sql_param_widgets.items():
            if isinstance(widget, QDateEdit):
                values[name] = widget.date().toString("dd/MM/yyyy")
            elif isinstance(widget, QLineEdit):
                values[name] = widget.text().strip()
            else:
                values[name] = ""
        return values

    def _execute_sql_query(self) -> None:
        """Executa a consulta SQL em thread separada."""
        if not self._sql_current_sql:
            self.show_error("Nenhum SQL", "Selecione um arquivo SQL antes de executar.")
            return
        if self.query_worker is not None and self.query_worker.isRunning():
            self.show_error("Aguarde", "Uma consulta já está em execução.")
            return

        values = self._collect_param_values()
        binds = self.sql_service.build_binds(self._sql_current_sql, values)

        self.btn_sql_execute.setEnabled(False)
        self._set_sql_status("⏳ Conectando ao Oracle...", "#fef9c3", "#92400e")

        self.query_worker = QueryWorker(self._sql_current_sql, binds)
        self.query_worker.progress.connect(lambda msg: self._set_sql_status(f"⏳ {msg}", "#fef9c3", "#92400e"))
        self.query_worker.finished_ok.connect(self._on_query_finished)
        self.query_worker.failed.connect(self._on_query_failed)
        self.query_worker.start()

    def _on_query_finished(self, df: pl.DataFrame) -> None:
        """Callback quando a consulta Oracle finaliza com sucesso."""
        self.btn_sql_execute.setEnabled(True)
        self._sql_result_df = df
        self._sql_result_page = 1
        if df.height == 0:
            self._set_sql_status("ℹ️  Consulta retornou 0 resultados.", "#e0e7ff", "#3730a3")
            self.sql_result_model.set_dataframe(pl.DataFrame())
        else:
            self._set_sql_status(
                f"✅ {df.height:,} linhas, {df.width} colunas.",
                "#dcfce7", "#166534"
            )
            self._show_sql_result_page()

    def _on_query_failed(self, message: str) -> None:
        """Callback quando a consulta Oracle falha."""
        self.btn_sql_execute.setEnabled(True)
        self._set_sql_status(f"❌ Erro: {message[:200]}", "#fee2e2", "#991b1b")

    def _set_sql_status(self, text: str, bg: str, fg: str) -> None:
        self.sql_status_label.setText(text)
        self.sql_status_label.setStyleSheet(
            f"QLabel {{ padding: 4px 8px; background: {bg}; border-radius: 4px; "
            f"border: 1px solid {bg}; color: {fg}; font-weight: bold; }}"
        )

    def _show_sql_result_page(self) -> None:
        """Exibe a página atual dos resultados SQL."""
        df = self._sql_result_df
        if df.height == 0:
            return
        total_pages = max(1, ((df.height - 1) // self._sql_result_page_size) + 1)
        self._sql_result_page = max(1, min(self._sql_result_page, total_pages))
        offset = (self._sql_result_page - 1) * self._sql_result_page_size
        page_df = df.slice(offset, self._sql_result_page_size)
        self.sql_result_model.set_dataframe(page_df)
        self.sql_result_table.resizeColumnsToContents()
        self.sql_result_page_label.setText(
            f"Página {self._sql_result_page}/{total_pages} | Total: {df.height:,}"
        )

    def _sql_prev_page(self) -> None:
        if self._sql_result_page > 1:
            self._sql_result_page -= 1
            self._show_sql_result_page()

    def _sql_next_page(self) -> None:
        total_pages = max(1, ((self._sql_result_df.height - 1) // self._sql_result_page_size) + 1)
        if self._sql_result_page < total_pages:
            self._sql_result_page += 1
            self._show_sql_result_page()

    def _filter_sql_results(self) -> None:
        """Aplica filtro textual global sobre os resultados SQL."""
        search = self.sql_result_search.text().strip().lower()
        if not search or self._sql_result_df.height == 0:
            self._sql_result_page = 1
            self._show_sql_result_page()
            return
        # Filtrar em todas as colunas (cast para string)
        exprs = [
            pl.col(c).cast(pl.Utf8, strict=False).fill_null("").str.to_lowercase().str.contains(search, literal=True)
            for c in self._sql_result_df.columns
        ]
        combined = exprs[0]
        for e in exprs[1:]:
            combined = combined | e
        filtered = self._sql_result_df.filter(combined)
        if filtered.height == 0:
            self._set_sql_status(f"ℹ️  Busca '{search}' não encontrou resultados.", "#e0e7ff", "#3730a3")
            self.sql_result_model.set_dataframe(pl.DataFrame())
        else:
            self._set_sql_status(
                f"✅ Busca '{search}': {filtered.height:,} de {self._sql_result_df.height:,} linhas.",
                "#dcfce7", "#166534"
            )
            # Show first page of filtered results
            page_df = filtered.head(self._sql_result_page_size)
            self.sql_result_model.set_dataframe(page_df)
            self.sql_result_table.resizeColumnsToContents()
            total_pages = max(1, ((filtered.height - 1) // self._sql_result_page_size) + 1)
            self.sql_result_page_label.setText(f"Página 1/{total_pages} | Filtrado: {filtered.height:,}")

    def _export_sql_results(self) -> None:
        """Exporta os resultados da consulta SQL para Excel."""
        if self._sql_result_df.height == 0:
            self.show_error("Sem dados", "Execute uma consulta antes de exportar.")
            return
        target = self._save_dialog("Exportar resultados SQL para Excel", "Excel (*.xlsx)")
        if not target:
            return
        try:
            sql_name = self.sql_combo.currentText().split("[")[0].strip() or "consulta_sql"
            self.export_service.export_excel(target, self._sql_result_df, sheet_name=sql_name[:31])
            self.show_info("Exportação concluída", f"Arquivo gerado em:\n{target}")
        except Exception as exc:
            self.show_error("Falha na exportação", str(exc))
