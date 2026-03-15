import { useAuth } from "@/_core/hooks/useAuth";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarInset,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
  SidebarTrigger,
  useSidebar,
} from "@/components/ui/sidebar";
import { getLoginUrl } from "@/const";
import { useIsMobile } from "@/hooks/useMobile";
import {
  Database,
  FileSpreadsheet,
  FileText,
  LayoutDashboard,
  LogOut,
  PanelLeft,
  Search,
  Settings,
  Table2,
  Puzzle,
  Shield,
  Layers,
  MousePointerClick
} from "lucide-react";
import { CSSProperties, useEffect, useRef, useState } from "react";
import { useLocation } from "wouter";
import { DashboardLayoutSkeleton } from "./DashboardLayoutSkeleton";
import { Button } from "./ui/button";
import { GlobalSearch } from "./ui/GlobalSearch";
import { motion, AnimatePresence } from "framer-motion";

const LOGO_URL =
  "https://d2xsxph8kpxj0f.cloudfront.net/310419663026769585/hbBC86DweQhXfHmYHxipLP/sefin-logo-ViDdyk2r8v7NSzj3XcttNr.webp";

const menuItems = [
  { icon: LayoutDashboard, label: "Dashboard", path: "/" },
  { icon: Shield, label: "Auditar CNPJ", path: "/auditar" },
  { icon: Layers, label: "Processamento em Lote", path: "/lote" },
  { icon: Database, label: "Extração Oracle", path: "/extracao" },
  { icon: Table2, label: "Visualizar Tabelas", path: "/tabelas" },
  { icon: MousePointerClick, label: "Agregação por Seleção", path: "/agregacao-selecao" },
  { icon: FileSpreadsheet, label: "Exportar Excel", path: "/exportar" },
  { icon: FileText, label: "Relatórios", path: "/relatorios" },
  { icon: Puzzle, label: "Análises", path: "/analises" },
  { icon: Settings, label: "Configurações", path: "/configuracoes" },
];

const SIDEBAR_WIDTH_KEY = "sidebar-width";
const DEFAULT_WIDTH = 260;
const MIN_WIDTH = 200;
const MAX_WIDTH = 400;

export default function DashboardLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const [sidebarWidth, setSidebarWidth] = useState(() => {
    const saved = localStorage.getItem(SIDEBAR_WIDTH_KEY);
    return saved ? parseInt(saved, 10) : DEFAULT_WIDTH;
  });
  const { loading, user } = useAuth();
  const isOAuthConfigured = Boolean(import.meta.env.VITE_OAUTH_PORTAL_URL);

  useEffect(() => {
    localStorage.setItem(SIDEBAR_WIDTH_KEY, sidebarWidth.toString());
  }, [sidebarWidth]);

  if (loading && isOAuthConfigured) {
    return <DashboardLayoutSkeleton />;
  }

  if (!user && isOAuthConfigured) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-background text-foreground">
        <div className="flex flex-col items-center gap-8 p-8 max-w-md w-full">
          <img src={LOGO_URL} alt="Sistema de Auditoria e Análise Fiscal" className="w-16 h-16" />
          <div className="flex flex-col items-center gap-3">
            <h1 className="text-2xl font-bold tracking-tight text-center bg-clip-text text-transparent bg-gradient-to-r from-primary to-emerald-600">
              Sistema de Auditoria e Análise Fiscal
            </h1>
            <p className="text-sm text-muted-foreground text-center max-w-sm">
              Sistema de Extração e Análise de Dados Fiscais — SEFIN/RO.
              Faça login para continuar.
            </p>
          </div>
          <Button
            onClick={() => {
              window.location.href = getLoginUrl();
            }}
            size="lg"
            className="w-full shadow-lg hover:shadow-xl transition-all"
          >
            Entrar
          </Button>
        </div>
      </div>
    );
  }

  return (
    <SidebarProvider
      style={
        {
          "--sidebar-width": `${sidebarWidth}px`,
        } as CSSProperties
      }
    >
      <DashboardLayoutContent setSidebarWidth={setSidebarWidth}>
        {children}
      </DashboardLayoutContent>
    </SidebarProvider>
  );
}

type DashboardLayoutContentProps = {
  children: React.ReactNode;
  setSidebarWidth: (width: number) => void;
};

function DashboardLayoutContent({
  children,
  setSidebarWidth,
}: DashboardLayoutContentProps) {
  const { user, logout } = useAuth();
  const [location, setLocation] = useLocation();
  const { state, toggleSidebar } = useSidebar();
  const isCollapsed = state === "collapsed";
  const [isResizing, setIsResizing] = useState(false);
  const sidebarRef = useRef<HTMLDivElement>(null);
  const activeMenuItem = menuItems.find((item) => item.path === location);
  const isMobile = useIsMobile();

  useEffect(() => {
    if (isCollapsed) {
      setIsResizing(false);
    }
  }, [isCollapsed]);

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isResizing) return;
      const sidebarLeft =
        sidebarRef.current?.getBoundingClientRect().left ?? 0;
      const newWidth = e.clientX - sidebarLeft;
      if (newWidth >= MIN_WIDTH && newWidth <= MAX_WIDTH) {
        setSidebarWidth(newWidth);
      }
    };

    const handleMouseUp = () => {
      setIsResizing(false);
    };

    if (isResizing) {
      document.addEventListener("mousemove", handleMouseMove);
      document.addEventListener("mouseup", handleMouseUp);
      document.body.style.cursor = "col-resize";
      document.body.style.userSelect = "none";
    }

    return () => {
      document.removeEventListener("mousemove", handleMouseMove);
      document.removeEventListener("mouseup", handleMouseUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };
  }, [isResizing, setSidebarWidth]);

  return (
    <>
      <div className="relative" ref={sidebarRef}>
        <Sidebar
          collapsible="icon"
          className="border-r-0"
          disableTransition={isResizing}
        >
          <SidebarHeader className="h-16 justify-center">
            <div className="flex items-center gap-3 px-2 transition-all w-full">
              <button
                onClick={toggleSidebar}
                className="h-8 w-8 flex items-center justify-center hover:bg-accent rounded-lg transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-ring shrink-0"
                aria-label="Alternar navegação"
              >
                {isCollapsed ? (
                  <img
                    src={LOGO_URL}
                    alt="SEFIN"
                    className="h-5 w-5 rounded"
                  />
                ) : (
                  <PanelLeft className="h-4 w-4 text-muted-foreground" />
                )}
              </button>
              {!isCollapsed ? (
                <div className="flex items-center gap-2 min-w-0">
                  <img
                    src={LOGO_URL}
                    alt="SEFIN"
                    className="h-6 w-6 rounded"
                  />
                  <span className="font-bold tracking-tight truncate text-sm">
                    Sistema de Auditoria e Análise Fiscal
                  </span>
                </div>
              ) : null}
            </div>
          </SidebarHeader>

          <SidebarContent className="gap-0">
            <SidebarMenu className="px-2 py-1">
              {menuItems.map((item) => {
                const isActive = location === item.path;
                return (
                  <SidebarMenuItem key={item.path}>
                    <SidebarMenuButton
                      isActive={isActive}
                      onClick={() => setLocation(item.path)}
                      tooltip={item.label}
                      className="h-10 transition-all font-normal"
                    >
                      <item.icon
                        className={`h-4 w-4 ${isActive ? "text-primary" : ""}`}
                      />
                      <span>{item.label}</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                );
              })}
            </SidebarMenu>
          </SidebarContent>

          <SidebarFooter className="p-3">
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <button className="flex items-center gap-3 rounded-lg px-1 py-1 hover:bg-accent/50 transition-colors w-full text-left group-data-[collapsible=icon]:justify-center focus:outline-none focus-visible:ring-2 focus-visible:ring-ring">
                  <Avatar className="h-9 w-9 border shrink-0">
                    <AvatarFallback className="text-xs font-medium bg-primary/10 text-primary">
                      {user?.name?.charAt(0)?.toUpperCase() ?? "U"}
                    </AvatarFallback>
                  </Avatar>
                  <div className="flex-1 min-w-0 group-data-[collapsible=icon]:hidden">
                    <p className="text-sm font-medium truncate leading-none">
                      {user?.name || "-"}
                    </p>
                    <p className="text-xs text-muted-foreground truncate mt-1.5">
                      {user?.email || "-"}
                    </p>
                  </div>
                </button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="w-48">
                <DropdownMenuItem
                  onClick={logout}
                  className="cursor-pointer text-destructive focus:text-destructive"
                >
                  <LogOut className="mr-2 h-4 w-4" />
                  <span>Sair</span>
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </SidebarFooter>
        </Sidebar>
        <div
          className={`absolute top-0 right-0 w-1 h-full cursor-col-resize hover:bg-primary/20 transition-colors ${isCollapsed ? "hidden" : ""}`}
          onMouseDown={() => {
            if (isCollapsed) return;
            setIsResizing(true);
          }}
          style={{ zIndex: 50 }}
        />
      </div>

      <SidebarInset>
        <header className="flex h-16 shrink-0 items-center justify-between border-b border-border/50 bg-background px-4 transition-[width,height] ease-linear group-has-[[data-collapsible=icon]]/sidebar-wrapper:h-12 sticky top-0 z-30">
          <div className="flex items-center gap-2">
            {!isMobile && <SidebarTrigger className="-ml-1" />}
            <div className="flex items-center gap-2 px-2">
              <span className="text-sm font-medium text-muted-foreground">
                {activeMenuItem?.label ?? "Dashboard"}
              </span>
            </div>
          </div>
          <div className="flex items-center gap-4">
            <GlobalSearch />
          </div>
        </header>
        <main className="flex-1 overflow-auto">
          <AnimatePresence mode="wait">
            <motion.div
              key={location}
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              transition={{ duration: 0.2, ease: "easeInOut" }}
              className="p-4 lg:p-6"
            >
              {children}
            </motion.div>
          </AnimatePresence>
        </main>
      </SidebarInset>
    </>
  );
}
