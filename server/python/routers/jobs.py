from fastapi import APIRouter, HTTPException, Query
from core.job_manager import job_manager
from typing import Optional

router = APIRouter(prefix="/api/python/jobs", tags=["jobs"])

@router.get("")
async def list_jobs(
    status: Optional[str] = Query(None, description="Filtrar por status do job"),
    tipo: Optional[str] = Query(None, description="Filtrar por tipo do job"),
    # cnpj/data would require searching params/execution traces for perfect match if not in JobState,
    # but we will provide basic filtering for now
):
    """Retorna a lista de todos os jobs com suporte a filtros básicos."""
    jobs = job_manager.list_jobs()

    if status:
        jobs = [j for j in jobs if j.status == status]
    if tipo:
        jobs = [j for j in jobs if j.job_type == tipo]

    # Retornar ordenado pelos mais recentes primeiro
    jobs.sort(key=lambda j: j.created_at, reverse=True)
    return {"success": True, "jobs": [j.model_dump() for j in jobs]}

@router.get("/{job_id}")
async def get_job_status(job_id: str):
    """Consulta o status e o progresso de um job específico."""
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado")
    return {"success": True, "job": job.model_dump()}

@router.post("/{job_id}/cancel")
async def cancel_job(job_id: str):
    """Solicita o cancelamento cooperativo de um job em andamento."""
    success = job_manager.cancel_job(job_id)
    if success:
        return {"success": True, "message": "Cancelamento solicitado com sucesso. O job irá abortar de forma segura em breve."}
    else:
         job = job_manager.get_job(job_id)
         if not job:
             raise HTTPException(status_code=404, detail="Job não encontrado")
         return {"success": False, "message": f"Não foi possível cancelar o job. Status atual: {job.status}"}
