import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  getAuditHistory,
  getAuditDetails,
  runAuditPipeline,
  getAuditStatus,
} from "@/lib/pythonApi";
import { toast } from "sonner";

export function useAuditHistory() {
  return useQuery({
    queryKey: ["auditHistory"],
    queryFn: async () => {
      const data = await getAuditHistory();
      if (!data.success) throw new Error("Falha ao buscar histórico");
      return data.historico;
    },
  });
}

export function useAuditDetails(cnpj: string | null) {
  return useQuery({
    queryKey: ["auditDetails", cnpj],
    queryFn: async () => {
      if (!cnpj) return null;
      return await getAuditDetails(cnpj);
    },
    enabled: !!cnpj,
    retry: 1,
  });
}

export function useRunAudit() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async ({
      cnpj,
      dataLimite,
    }: {
      cnpj: string;
      dataLimite: string;
    }) => {
      return await runAuditPipeline(cnpj, dataLimite);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["auditHistory"] });
    },
  });
}

export function useAuditPolling(cnpj: string | null) {
  return useQuery({
    queryKey: ["auditPolling", cnpj],
    queryFn: async () => {
      if (!cnpj) return null;
      return await getAuditStatus(cnpj);
    },
    enabled: !!cnpj,
    refetchInterval: query => {
      const data = query.state.data as any;
      if (!data) return 3000;
      const status = data.job_status;
      if (status === "agendada" || status === "executando") return 3000;
      return false;
    },
  });
}
