import re

with open('client/src/hooks/useAuditoria.ts', 'r') as f:
    content = f.read()

# Fix React Query v5 refetchInterval signature
content = content.replace(
"""        refetchInterval: (data) => {
            if (!data?.state?.data) return 3000;
            const status = (data.state.data as any).job_status;
            if (status === "agendada" || status === "executando") return 3000;
            return false;
        },""",
"""        refetchInterval: (query) => {
            const data = query.state.data as any;
            if (!data) return 3000;
            const status = data.job_status;
            if (status === "agendada" || status === "executando") return 3000;
            return false;
        },"""
)

with open('client/src/hooks/useAuditoria.ts', 'w') as f:
    f.write(content)
