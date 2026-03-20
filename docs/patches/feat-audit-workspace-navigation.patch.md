# Patch manual para fechar a implementação da navegação

Este branch já contém os novos arquivos:

- `client/src/pages/AuditoriaWorkspace.tsx`
- `client/src/pages/AuditoriaExecucoes.tsx`
- `client/src/pages/AuditoriaCasos.tsx`
- `client/src/pages/TratamentoProdutosHub.tsx`

Como o conector disponível criou bem os arquivos novos, mas não editou com segurança os arquivos existentes, faltam **duas alterações cirúrgicas**.

---

## 1) Atualizar `client/src/App.tsx`

### Importações novas
Adicione estas importações:

```tsx
import AuditoriaWorkspace from "./pages/AuditoriaWorkspace";
import AuditoriaExecucoes from "./pages/AuditoriaExecucoes";
import AuditoriaCasos from "./pages/AuditoriaCasos";
import TratamentoProdutosHub from "./pages/TratamentoProdutosHub";
```

### Rotas novas
Dentro do `DashboardLayout`, adicione estas rotas **antes** das rotas antigas equivalentes:

```tsx
<Route path="/auditoria" component={AuditoriaWorkspace} />
<Route path="/auditoria/execucoes" component={AuditoriaExecucoes} />
<Route path="/auditoria/casos" component={AuditoriaCasos} />
<Route path="/tratamento-produtos" component={TratamentoProdutosHub} />
```

### Bloco de exemplo
O trecho relevante deve ficar assim:

```tsx
<Route path="/" component={Home} />
<Route path="/auditoria" component={AuditoriaWorkspace} />
<Route path="/auditoria/execucoes" component={AuditoriaExecucoes} />
<Route path="/auditoria/casos" component={AuditoriaCasos} />
<Route path="/tratamento-produtos" component={TratamentoProdutosHub} />
<Route path="/auditar" component={AuditarCNPJ} />
<Route path="/lote" component={LoteAuditoria} />
<Route path="/extracao" component={Extracao} />
<Route path="/tabelas" component={Tabelas} />
```

---

## 2) Atualizar `client/src/components/DashboardLayout.tsx`

### Importações de ícones
Troque o bloco de ícones para incluir os novos itens:

```tsx
import {
  Boxes,
  Database,
  FileSpreadsheet,
  FileText,
  FolderTree,
  LayoutDashboard,
  LogOut,
  PanelLeft,
  Puzzle,
  Settings,
  Shield,
  Table2,
  ClipboardList,
  PlayCircle,
} from "lucide-react";
```

### Novo `menuItems`
Substitua o array atual por este:

```tsx
const menuItems = [
  { icon: LayoutDashboard, label: "Dashboard", path: "/" },
  { icon: Shield, label: "Auditoria", path: "/auditoria" },
  { icon: PlayCircle, label: "Execuções", path: "/auditoria/execucoes" },
  { icon: ClipboardList, label: "Casos Auditados", path: "/auditoria/casos" },
  { icon: FolderTree, label: "Tratamento Produtos", path: "/tratamento-produtos" },
  { icon: Layers, label: "Processamento em Lote", path: "/lote" },
  { icon: Database, label: "Extracao Oracle", path: "/extracao" },
  { icon: Table2, label: "Visualizar Tabelas", path: "/tabelas" },
  { icon: Boxes, label: "Analise de Produtos", path: "/analise-produtos" },
  { icon: FileSpreadsheet, label: "Exportar Excel", path: "/exportar" },
  { icon: FileText, label: "Relatorios", path: "/relatorios" },
  { icon: Puzzle, label: "Analises", path: "/analises" },
  { icon: Settings, label: "Configuracoes", path: "/configuracoes" },
];
```

> Observação: `Layers` já existe no arquivo atual e deve ser mantido.

---

## Resultado prático desta implementação

Depois dessas duas edições:

- o sistema ganha uma entrada nova em `/auditoria`
- a auditoria fica separada em:
  - `/auditoria/execucoes`
  - `/auditoria/casos`
- o tratamento de produtos ganha um hub em `/tratamento-produtos`
- as rotas antigas continuam funcionando
- a refatoração pesada pode acontecer depois, sem quebrar o uso atual

---

## Próximo passo recomendado

Depois de aplicar este patch, o próximo passo natural é dividir `AnaliseProdutos.tsx` em:

- `tratamento_produtos/revisao_final`
- `tratamento_produtos/fatores`
- `tratamento_produtos/sugestoes`
- `tratamento_produtos/avancado`

Sem essa etapa, a nova arquitetura já existe, mas o motor operacional ainda continua concentrado na página antiga.
