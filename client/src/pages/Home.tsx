import { useAuth } from "@/_core/hooks/useAuth";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Activity,
  BarChart3,
  Database,
  FileText,
  Play,
  RotateCcw,
  Shield,
  Layers,
  Table2,
  ArrowRight,
} from "lucide-react";
import { useAuditHistory } from "@/hooks/useAuditoria";
import { motion } from "framer-motion";
import { format } from "date-fns";
import { ptBR } from "date-fns/locale";
import { Link } from "wouter";

export default function Home() {
  const { user } = useAuth();
  const { data: history, isLoading: loadingHistory } = useAuditHistory();

  const quickActions = [
    {
      title: "Nova Auditoria (1 CNPJ)",
      description: "Inicie a extração e análise completa para uma única empresa.",
      icon: Shield,
      href: "/auditar",
      color: "text-emerald-500",
      bg: "bg-emerald-500/10",
      border: "border-emerald-500/20",
    },
    {
      title: "Processamento em Lote",
      description: "Audite múltiplos CNPJs de uma só vez para maior eficiência.",
      icon: Layers,
      href: "/lote",
      color: "text-blue-500",
      bg: "bg-blue-500/10",
      border: "border-blue-500/20",
    },
    {
      title: "Ver Tabelas",
      description: "Acesse e analise os dados das empresas já processadas.",
      icon: Table2,
      href: "/tabelas",
      color: "text-amber-500",
      bg: "bg-amber-500/10",
      border: "border-amber-500/20",
    },
    {
      title: "Ver Relatórios",
      description: "Gere e consulte relatórios em Word das auditorias concluídas.",
      icon: FileText,
      href: "/relatorios",
      color: "text-purple-500",
      bg: "bg-purple-500/10",
      border: "border-purple-500/20",
    },
  ];

  return (
    <div className="space-y-8 max-w-7xl mx-auto">
      {/* Header Section */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div className="space-y-1">
          <h1 className="text-3xl font-extrabold tracking-tight text-gradient">
            Centro de Comando
          </h1>
          <p className="text-muted-foreground">
            Bem-vindo ao Sistema de Auditoria e Análise Fiscal — SEFIN/RO
          </p>
        </div>
        <div className="flex items-center gap-2 text-sm text-muted-foreground glass-card px-4 py-2 rounded-full">
          <Badge variant="outline" className="bg-emerald-500/10 text-emerald-500 border-emerald-500/20 text-[10px]">
            CONECTADO
          </Badge>
          <span>Produção Oracle</span>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        {/* Ações Rápidas - Quick Actions */}
        <div className="lg:col-span-12 xl:col-span-6 space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {quickActions.map((action, idx) => (
              <motion.div
                key={action.title}
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: idx * 0.1 }}
                className="h-full"
              >
                <Link href={action.href}>
                  <Card className="glass-card h-full overflow-hidden group hover:shadow-xl hover:shadow-primary/5 hover:border-primary/20 transition-all duration-300 cursor-pointer flex flex-col justify-between">
                    <CardContent className="p-6">
                      <div className="flex flex-col gap-4">
                        <div className={`p-3 rounded-xl w-fit ${action.bg} ${action.border} border group-hover:scale-110 transition-transform duration-300`}>
                          <action.icon className={`h-6 w-6 ${action.color}`} />
                        </div>
                        <div className="space-y-2">
                          <h3 className="text-lg font-bold tracking-tight group-hover:text-primary transition-colors">
                            {action.title}
                          </h3>
                          <p className="text-sm text-muted-foreground">
                            {action.description}
                          </p>
                        </div>
                      </div>
                      <div className="mt-6 flex items-center text-sm font-medium text-primary gap-1 group-hover:gap-2 transition-all">
                        Acessar <ArrowRight className="h-4 w-4" />
                      </div>
                    </CardContent>
                  </Card>
                </Link>
              </motion.div>
            ))}
          </div>

          {/* Dica / Info */}
          <div className="p-5 rounded-2xl bg-gradient-to-br from-primary/10 to-emerald-500/5 border border-primary/5 flex items-start gap-4 shadow-sm">
            <div className="h-10 w-10 rounded-xl bg-primary/10 flex items-center justify-center shrink-0">
              <BarChart3 className="h-5 w-5 text-primary" />
            </div>
            <div className="space-y-1">
              <p className="text-sm font-bold">Dica do Sistema</p>
              <p className="text-xs text-muted-foreground leading-relaxed">
                Utilize o módulo de **Relatórios** para gerar documentos Word automáticos com base nos resultados das auditorias concluídas.
              </p>
            </div>
          </div>
        </div>

        {/* Auditorias Recentes - List */}
        <div className="lg:col-span-12 xl:col-span-6">
          <Card className="glass-card h-full">
            <CardHeader className="flex flex-row items-center justify-between">
              <div className="space-y-1">
                <CardTitle className="text-lg">Auditorias Recentes</CardTitle>
                <p className="text-xs text-muted-foreground">
                  Acompanhamento das últimas empresas processadas
                </p>
              </div>
              <Link href="/tabelas">
                <Button variant="ghost" size="sm" className="text-xs gap-1 h-8">
                  <RotateCcw className="h-3 w-3" />
                  Ver tudo
                </Button>
              </Link>
            </CardHeader>
            <CardContent>
              <div className="space-y-1">
                {loadingHistory ? (
                  Array(5).fill(0).map((_, i) => (
                    <div key={i} className="h-14 w-full animate-pulse bg-muted/40 rounded-xl mb-2" />
                  ))
                ) : history && history.length > 0 ? (
                  history.slice(0, 7).map((audit, idx) => (
                    <motion.div
                      key={audit.cnpj}
                      initial={{ opacity: 0, x: 20 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: idx * 0.05 }}
                      className="flex items-center justify-between p-3 rounded-xl hover:bg-muted/30 transition-colors group cursor-pointer"
                    >
                      <div className="flex items-center gap-3">
                        <div className="h-9 w-9 rounded-lg bg-background border flex items-center justify-center shadow-sm group-hover:border-primary/30 transition-colors">
                          <Database className="h-4 w-4 text-muted-foreground" />
                        </div>
                        <div>
                          <p className="text-sm font-bold tracking-tight">
                            {audit.cnpj}
                          </p>
                          <p className="text-[10px] text-muted-foreground">
                             {audit.razao_social || "CNPJ Processado"}
                          </p>
                        </div>
                      </div>
                      <div className="text-right">
                        <div className="text-[10px] font-medium text-muted-foreground">
                          {audit.ultima_modificacao ? format(new Date(audit.ultima_modificacao), "dd/MM/yyyy HH:mm", { locale: ptBR }) : '-'}
                        </div>
                        <div className="flex gap-1 mt-1 justify-end">
                           <Badge variant="outline" className="text-[9px] px-1 py-0 h-4 border-emerald-500/20 bg-emerald-500/5 text-emerald-600">
                             {audit.qtd_parquets} Tab
                           </Badge>
                           <Badge variant="outline" className="text-[9px] px-1 py-0 h-4 border-blue-500/20 bg-blue-500/5 text-blue-600">
                             {audit.qtd_relatorios} Rel
                           </Badge>
                        </div>
                      </div>
                    </motion.div>
                  ))
                ) : (
                  <div className="flex flex-col items-center justify-center py-12 text-center space-y-3">
                    <div className="h-12 w-12 rounded-full bg-muted/30 flex items-center justify-center">
                      <FileText className="h-6 w-6 text-muted-foreground" />
                    </div>
                    <div className="space-y-1">
                      <p className="text-sm font-medium">Nenhuma auditoria encontrada</p>
                      <p className="text-xs text-muted-foreground">
                        Inicie sua primeira análise no card ao lado.
                      </p>
                    </div>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
