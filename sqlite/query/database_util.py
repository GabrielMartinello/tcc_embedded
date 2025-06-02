import time
import psutil
import os
from .db_sqlite import get_conn
from tabulate import tabulate

con = get_conn()
cursor = con.cursor()
process = psutil.Process(os.getpid())

def imprimir_resultados(result, colunas,end, start, mem_before, mem_after, cpu):
    print(tabulate(result, headers=colunas, tablefmt="psql"))
    print("\n")
    print(f"Tempo da query: {end - start:.4f} segundos")
    print(f"Memória antes: {mem_before:.2f} MB")
    print(f"Memória depois: {mem_after:.2f} MB")
    print(f"Variação de memória: {mem_after - mem_before:.2f} MB")
    print(f"CPU (percentual instantâneo): {cpu}%")
    print("\n")


def executar_consulta(query):
    mem_before = process.memory_info().rss / (1024 ** 2)
    tic=time.time()
    result = cursor.execute(query).fetchall()
    toc=time.time()
    mem_after = process.memory_info().rss / (1024 ** 2)
    cpu_after = process.cpu_percent(interval=None)

    colunas = [desc[0] for desc in cursor.description]

    imprimir_resultados(result, colunas,toc, tic, mem_before, mem_after, cpu_after)