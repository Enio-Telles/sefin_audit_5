import sys
sys.path.append('server/python/')
from routers.produto_unid import _similarity_score

print(_similarity_score("CERVEJA BRAHMA LATA 350ML", "CERVEJA BRAHMA LT 350ML"))
print(_similarity_score("CERVEJA SKOL LATA 350ML", "CERVEJA BRAHMA LT 350ML"))
