import duckdb
import time
from query import queries
import os
import platform
import psutil

print("="*40, "Informações do Sistema", "="*40)
print(" "*40, f"Sistema Operacional: {platform.system()} {platform.release()}")
print(" "*40, f"Versão: {platform.version()}")
print(" "*40, f"Arquitetura: {platform.machine()}")
print(" "*40, f"Processador: {platform.processor()}")
print(" "*40, f"Núcleos físicos: {psutil.cpu_count(logical=False)}")
print(" "*40, f"Núcleos lógicos (threads): {psutil.cpu_count(logical=True)}")

mem = psutil.virtual_memory()
print(" "*40, f"Memória RAM Total: {round(mem.total / (1024**3), 2)} GB")
print(" "*40, f"Memória RAM Disponível: {round(mem.available / (1024**3), 2)} GB")
print(" "*40, f"Uso de Memória: {mem.percent}%")

disk = psutil.disk_usage('/')
print(" "*40, f"Disco Total: {round(disk.total / (1024**3), 2)} GB")
print(" "*40, f"Disco Usado: {round(disk.used / (1024**3), 2)} GB")
print(" "*40, f"Disco Livre: {round(disk.free / (1024**3), 2)} GB")
print(" "*40, f"Uso do Disco: {disk.percent}%")
print("="*80)
print("\n"*3)
con = duckdb.connect("banco_bagre.duckdb")

print("Quantidade total de linhas:.....")
tic=time.time()
quantidade_de_linhas = con.execute(queries.query_count_todos_registros).fetchdf()
toc=time.time()
print(quantidade_de_linhas)
print(F"A consulta demorou {toc-tic} segundos")
print("\n"*3)

print("Registros do sistema:.....")
tic=time.time()
tipos_de_registros = con.execute(queries.query_qtde_registros_eventos_sistema).fetchdf()
toc=time.time()
print(tipos_de_registros)
print(F"A consulta demorou {toc-tic} segundos")
print("\n"*2)


print("Quantidade de Erros Alerta e Infos:.....")
tic=time.time()
erros_alertas_infos = con.execute(queries.query_qtde_erro_alerta_info).fetchdf()
toc=time.time()
print(erros_alertas_infos)
print(F"A consulta demorou {toc-tic} segundos\n")

print("\n"*2)
print( "="*40, "EXECUTANDO CONSULTAS", "="*40)
print("\n"*3)