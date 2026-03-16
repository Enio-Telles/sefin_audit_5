import { Toaster } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import NotFound from "@/pages/NotFound";
import { Route, Switch } from "wouter";
import ErrorBoundary from "./components/ErrorBoundary";
import { ThemeProvider } from "./contexts/ThemeContext";
import DashboardLayout from "./components/DashboardLayout";
import Home from "./pages/Home";
import Extracao from "./pages/Extracao";
import Tabelas from "./pages/Tabelas";
import Exportar from "./pages/Exportar";
import Relatorios from "./pages/Relatorios";
import Analises from "./pages/Analises";
import Configuracoes from "./pages/Configuracoes";
import AnaliseFaturamentoPeriodo from "./pages/AnaliseFaturamentoPeriodo";
import AuditarCNPJ from "./pages/AuditarCNPJ";
import LoteAuditoria from "./pages/LoteAuditoria";
import AnaliseProdutos from "./pages/AnaliseProdutos";
import RevisaoFatores from "./pages/RevisaoFatores";
import UnificarProdutoPage from "./pages/UnificarProdutoPage";
import MultiUnificarProdutoPage from "./pages/MultiUnificarProdutoPage";
import DesagregarProdutoPage from "./pages/DesagregarProdutoPage";

function Router() {
  return (
    <Switch>
      <Route path="/unificar/:cnpj/:codigo" component={UnificarProdutoPage} />
      <Route path="/unificar-multi/:cnpj" component={MultiUnificarProdutoPage} />
      <Route path="/desagregar/:cnpj/:codigo" component={DesagregarProdutoPage} />
      <Route>
        <DashboardLayout>
          <Switch>
            <Route path="/" component={Home} />
            <Route path="/auditar" component={AuditarCNPJ} />
            <Route path="/lote" component={LoteAuditoria} />
            <Route path="/extracao" component={Extracao} />
            <Route path="/tabelas" component={Tabelas} />
            <Route path="/tabelas/view" component={Tabelas} />
            <Route path="/analise-produtos" component={AnaliseProdutos} />
            <Route path="/revisao-manual" component={AnaliseProdutos} />
            <Route path="/revisao-pares-grupos" component={AnaliseProdutos} />
            <Route path="/agregacao-selecao" component={AnaliseProdutos} />
            <Route path="/revisao-fatores" component={RevisaoFatores} />
            <Route path="/exportar" component={Exportar} />
            <Route path="/relatorios" component={Relatorios} />
            <Route path="/analises" component={Analises} />
            <Route path="/analises/faturamento-periodo" component={AnaliseFaturamentoPeriodo} />

            <Route path="/configuracoes" component={Configuracoes} />
            <Route path="/404" component={NotFound} />
            <Route component={NotFound} />
          </Switch>
        </DashboardLayout>
      </Route>
    </Switch>
  );
}

function App() {
  return (
    <ErrorBoundary>
      <ThemeProvider defaultTheme="dark">
        <TooltipProvider>
          <Toaster />
          <Router />
        </TooltipProvider>
      </ThemeProvider>
    </ErrorBoundary>
  );
}

export default App;
