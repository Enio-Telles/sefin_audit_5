import sys
import time
sys.path.append('server/python/')
from routers.produto_unid import _similarity_score

start_time = time.time()
for _ in range(10000):
    _similarity_score("CERVEJA BRAHMA LATA 350ML", "CERVEJA BRAHMA LT 350ML")
    _similarity_score("CERVEJA SKOL LATA 350ML", "CERVEJA BRAHMA LT 350ML")
    _similarity_score("CERVEJA SKOL LATA 350ML", "CERVEJA SKOL LT 350ML")

print(f"Time taken: {time.time() - start_time} seconds")
