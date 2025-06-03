import monetdblite
import time
import os
import sys
import polars as pl
import glob
import os
import time
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from gerar_json import registrar_benchmark_carga
from resource_monitor import ResourceMonitor
import psutil
from query.db_monetdb import conn 

base_path = os.path.abspath("../data/logs")
pattern = os.path.join(base_path, "2_*", "*_new.csv")
files = glob.glob(pattern)

if not files:
    raise FileNotFoundError("Nenhum arquivo CSV encontrado.")

ACCEPTED_DATES = [
    '2024-10-27', '2024-11-29',
    '2024-10-28', '2024-11-30',
]

FILTROS_EXACT = [
    'Aguardando digitação do identificador do eleitor',
    'Identificador do eleitor digitado pelo mesário',
    'Eleitor foi habilitado',
    'O voto do eleitor foi computado',
    'Solicitação de dado pessoal do eleitor para habilitação manual'
]

FILTROS_LIKE = [
    'Zona Eleitoral%',
    'Seção Eleitoral%',
    'Município%',
    'Local de Votação%',
    'Turno da UE%',
    'Identificação do Modelo de Urna%',
    'Urna pronta para receber vot%',
    'Voto confirmado par%',
    '%Digital%',
    'Tipo de habilitação do eleitor [biométrica]%',
    'Solicita digital%'
]

print("Lendo arquivos CSV...")
process = psutil.Process(os.getpid())
monitor = ResourceMonitor(process, interval=0.2)  # Mede a cada 200ms
monitor.start()
start = time.perf_counter()

dfs = []
for file in files:
    df_temp = pl.read_csv(
        file,
        separator="\t",
        encoding="utf8-lossy",
        has_header=False
    ).with_columns(
        pl.lit(file).alias("filename")
    )
    dfs.append(df_temp)

df = pl.concat(dfs)

df = df.rename({
    "column_1": "event_timestamp",
    "column_2": "event_type",
    "column_3": "some_id",
    "column_4": "event_system",
    "column_5": "event_description",
    "column_6": "event_id"
})

df = df.with_columns([
    pl.col("filename").str.extract(r'2_([A-Z]{2})', 1).alias("uf"),
    pl.col("filename").str.extract(r'([^\\/]+)$', 1).alias("filename_only")
])

df = df.with_columns([
    pl.col("event_timestamp").str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M:%S", strict=False),
    pl.col("filename_only").str.slice(8, 5).alias("city_code"),  
    pl.col("filename_only").str.slice(13, 4).alias("zone_code"),   
    pl.col("filename_only").str.slice(17, 4).alias("section_code"), 
])

df = df.with_columns(
    pl.col("event_timestamp").dt.date().alias("event_date")
)

df = df.with_columns(
    pl.col("event_description").str.strip_chars().str.to_lowercase()
)

filtros_exact_lower = [f.lower() for f in FILTROS_EXACT]
filtros_like_lower = [f.lower() for f in FILTROS_LIKE]

print("Aplicando filtros no Polars...")

accepted_dates_dt = [datetime.strptime(d, "%Y-%m-%d").date() for d in ACCEPTED_DATES]

like_conditions = []
for pattern in filtros_like_lower:
    if pattern.startswith('%') and pattern.endswith('%'):
        like_conditions.append(
            pl.col("event_description").str.contains(pattern.strip('%'), literal=False)
        )
    elif pattern.endswith('%'):
        like_conditions.append(
            pl.col("event_description").str.starts_with(pattern.rstrip('%'))
        )
    elif pattern.startswith('%'):
        like_conditions.append(
            pl.col("event_description").str.ends_with(pattern.lstrip('%'))
        )
    else:
        like_conditions.append(pl.col("event_description") == pattern)

like_filter = pl.any_horizontal(like_conditions) if like_conditions else pl.lit(True)
exact_filter = pl.col("event_description").is_in(filtros_exact_lower)
date_filter = pl.col("event_date").is_in(accepted_dates_dt)

df_filtered = df.filter(
    (exact_filter | like_filter) & date_filter
)

print(f"Linhas após filtro: {df_filtered.shape[0]}")

df = df.with_columns(
    pl.col("event_timestamp").dt.strftime("%Y-%m-%d %H:%M:%S.%3f").alias("event_timestamp_str"),
    pl.col("event_date").dt.strftime("%Y-%m-%d").alias("event_date")
)

output_csv = "./events_df.csv"
df_filtered.write_csv(output_csv)
# Remover cabeçalho do CSV
with open(output_csv, 'r', encoding='utf8') as f:
    lines = f.readlines()

with open(output_csv, 'w', encoding='utf8') as f:
    f.writelines(lines[1:])  # Remove o cabeçalho
print(f"CSV tratado salvo em: {output_csv}")

#monetdblite.sql("""
#DROP TABLE eventos;
#""", client=conn)

monetdblite.sql("""
CREATE TABLE eventos (
    event_timestamp TIMESTAMP,
    event_type STRING,
    some_id STRING,
    event_system STRING,
    event_description STRING,
    event_id STRING,
    filename STRING,
    uf STRING,
    filename_only STRING,
    city_code STRING,
    zone_code STRING,
    section_code STRING,
    event_date DATE
);
""", client=conn)

# Carregando CSV
csv_path = os.path.abspath('./events_df.csv')
monetdblite.sql(f"""
COPY INTO eventos
FROM '{csv_path}'
USING DELIMITERS ',', '\n', '\"' NULL AS '';
""", client=conn)

print("CSV carregado no MonetDBLite com sucesso.")
end = time.perf_counter()
monitor.stop()
monitor.join()

tempo_execucao = end - start

cpu_percent_medio = monitor.get_average_cpu()
mem_before = monitor.mem_readings[0] if monitor.mem_readings else 0
mem_after = monitor.mem_readings[-1] if monitor.mem_readings else 0
mem_max = monitor.get_max_memory()

tamanho_total_bytes = sum(os.path.getsize(f) for f in files)
tamanho_total_mb = tamanho_total_bytes / (1024 * 1024)

print(f"\nProcesso concluído em {tempo_execucao:.2f} segundos")
print(f"Memória início: {mem_before:.2f} MB | final: {mem_after:.2f} MB | pico: {mem_max:.2f} MB")
print(f"CPU percentual médio durante execução: {cpu_percent_medio:.2f}%")
print(f"\nProcesso concluído em {end - start:.2f} segundos")

registrar_benchmark_carga(
    banco="MonetDBLite",
    tempo_execucao=tempo_execucao,
    linhas=df_filtered.shape[0],
    arquivos=len(files),
    tamanho_total_mb=tamanho_total_mb,
    mem_before=mem_before,
    mem_after=mem_after,
    cpu_percent=cpu_percent_medio
)