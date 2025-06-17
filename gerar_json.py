import psutil
import os
import json
from pathlib import Path
from datetime import datetime

RESULTS_FILE = Path("../resultados_benchmark.json")
process = psutil.Process(os.getpid())

def get_hardware_info():
    return {
        "cpu_model": os.cpu_count(),
        "cores_fisicos": psutil.cpu_count(logical=False),
        "threads": psutil.cpu_count(logical=True),
        "ram_total": f"{round(psutil.virtual_memory().total / (1024 ** 3))}GB",
        "sistema_operacional": os.name
    }

def salvar_resultados_consultas(banco_nome, dados_consulta):
    if RESULTS_FILE.exists():
        with open(RESULTS_FILE, "r") as file:
            resultados = json.load(file)
    else:
        resultados = {"hardware": get_hardware_info(), "bancos": []}

    banco_existente = next((b for b in resultados["bancos"] if b["nome"] == banco_nome.lower()), None)

    if banco_existente:
        banco_existente["consultas"].append(dados_consulta)
    else:
        resultados["bancos"].append({
            "nome": banco_nome.lower(),
            "consultas": [dados_consulta]
        })

    with open(RESULTS_FILE, "w") as file:
        json.dump(resultados, file, indent=4)

def registrar_benchmark_carga(banco, tempo_execucao, linhas, arquivos, tamanho_total_mb, mem_before, mem_after, cpu_percent, mex_max):
    json_file = "../benchmark_cargas.json"
    data = {}

    if os.path.exists(json_file):
        with open(json_file, 'r') as f:
            data = json.load(f)

    if 'cargas' not in data:
        data['cargas'] = []

    data['cargas'].append({
        "banco": banco,
        "data_execucao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tempo_execucao_segundos": round(tempo_execucao, 2),
        "linhas_carregadas": linhas,
        "arquivos_lidos": arquivos,
        "tamanho_total_arquivos_mb": round(tamanho_total_mb, 2),
        "memoria_inicio_mb": round(mem_before, 2),
        "memoria_final_mb": round(mem_after, 2),
        "memoria_variacao_mb": round(mem_after - mem_before, 2),
        "cpu_percent_medio": cpu_percent,
        "memoria_maxima_utilizada": mex_max
    })

    with open(json_file, 'w') as f:
        json.dump(data, f, indent=4)        