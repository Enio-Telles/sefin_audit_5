from fastapi import APIRouter
from .revisao import router as revisao_router
from .vectorizacao import router as vectorizacao_router
from .status import router as status_router
from .multidescricao import router as multidescricao_router
from .lote import router as lote_router
from .detalhes import router as detalhes_router
from .decisoes import router as decisoes_router
from .pares_similares import router as pares_similares_router

router = APIRouter(prefix="/api/python", tags=["produto_unid"])

router.include_router(revisao_router, include_in_schema=False)
router.include_router(vectorizacao_router, include_in_schema=False)
router.include_router(status_router, include_in_schema=False)
router.include_router(multidescricao_router, include_in_schema=False)
router.include_router(lote_router, include_in_schema=False)
router.include_router(detalhes_router, include_in_schema=False)
router.include_router(decisoes_router, include_in_schema=False)
router.include_router(pares_similares_router, include_in_schema=False)
