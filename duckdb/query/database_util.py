import time
import psutil
import os
from .db_duck import con

process = psutil.Process(os.getpid())

def imprimir_resultados(result, end, start, mem_before, mem_after, cpu):
    print(f"{result}")
    print("\n")
    print(f"Tempo da query: {end - start:.4f} segundos")
    print(f"Memória antes: {mem_before:.2f} MB")
    print(f"Memória depois: {mem_after:.2f} MB")
    print(f"Variação de memória: {mem_after - mem_before:.2f} MB")
    print(f"CPU (percentual instantâneo): {cpu}%")
    print("\n")


def executar_consulta(query):
    mem_before = process.memory_info().rss / (1024 ** 2)
    cpu_before = process.cpu_percent(interval=None)
    tic=time.time()
    result = con.execute(query).fetchdf()
    toc=time.time()
    mem_after = process.memory_info().rss / (1024 ** 2)
    cpu_after = process.cpu_percent(interval=None)
    imprimir_resultados(result, toc, tic, mem_before, mem_after, cpu_after)