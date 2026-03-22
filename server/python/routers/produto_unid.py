from fastapi import APIRouter
from .produtos.revisao import router as revisao_router
from .produtos.vectorizacao import router as vectorizacao_router
from .produtos.status import router as status_router
from .produtos.multidescricao import router as multidescricao_router
from .produtos.lote import router as lote_router
from .produtos.detalhes import router as detalhes_router
from .produtos.decisoes import router as decisoes_router
from .produtos.pares_similares import router as pares_similares_router

# Mantemos a prefixo /api/python e tag original
router = APIRouter(prefix="/api/python", tags=["produto_unid"])

# Incluimos as rotas (elas ja tem /produtos/ no caminho que foram importados dos modulos)
router.include_router(revisao_router)
router.include_router(vectorizacao_router)
router.include_router(status_router)
router.include_router(multidescricao_router)
router.include_router(lote_router)
router.include_router(detalhes_router)
router.include_router(decisoes_router)
router.include_router(pares_similares_router)
