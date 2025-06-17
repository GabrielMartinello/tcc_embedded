import queries
from database_util import executar_consulta 
import os
import platform
import psutil

process = psutil.Process(os.getpid())

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

executar_consulta(queries.query_count_todos_registros, "Quantidade total de linhas")
executar_consulta(queries.query_qtde_registros_eventos_sistema, "Todos os tipos de registros")
executar_consulta(queries.query_qtde_erro_alerta_info, "Contagem de erros, alertas e infos")

print( "="*40, "EXECUTANDO CONSULTAS DE AGREGAÇÃO", "="*40)

print("\n")

executar_consulta(queries.query_top_10_pessoas_que_mais_demoraram, "Top 10 pessoas que mais demoraram para votar")
executar_consulta(queries.query_tempo_medio_voto_por_zona, "Tempo medio de voto por zonas")
executar_consulta(queries.query_tempo_medio_voto_uf, "Tempo medio de voto por estados")
executar_consulta(queries.query_tempo_medio_voto_cidade, "Tempo medio de voto por cidades")