import time
import psutil
import sys
import os
import chdb

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from gerar_json import salvar_resultados_consultas
from resource_monitor import ResourceMonitor

con = chdb.connect()
con.query("SET allow_experimental_join_condition = 1;")
process = psutil.Process(os.getpid())
banco_nome = "CHDB"

def executar_consulta(query, descricao=""):
    process = psutil.Process(os.getpid())
    monitor = ResourceMonitor(process, interval=0.2)
    monitor.start()

    tic = time.perf_counter()
    result = con.query(query)
    
    print(result)
    toc = time.perf_counter()
    monitor.stop()
    monitor.join()

    cpu_percent_medio = monitor.get_average_cpu()
    mem_before = monitor.mem_readings[0] if monitor.mem_readings else 0
    mem_after = monitor.mem_readings[-1] if monitor.mem_readings else 0
    mem_max = monitor.get_max_memory()

    tempo_exec = round(toc - tic, 4)

    dados_consulta = {
        "descricao": descricao,
        "tempo_execucao_s": tempo_exec,
        "memoria_inicial_mb": round(mem_before, 2),
        "memoria_final_mb": round(mem_after, 2),
        "memoria_max_utilizada": mem_max,
        "cpu_percent": cpu_percent_medio
    }

    salvar_resultados_consultas(banco_nome, dados_consulta)

    print(f"\n=== {banco_nome} | {descricao} ===")
    print(f"Tempo de execução: {tempo_exec}s")
    print(f"Memória antes: {round(mem_before,2)} MB")
    print(f"Memória depois: {round(mem_after,2)} MB")
    print(f"Memória máxima utilizada: {mem_max} MB")
    print(f"Uso médio de CPU (pós consulta): {cpu_percent_medio}%\n")

    return result
