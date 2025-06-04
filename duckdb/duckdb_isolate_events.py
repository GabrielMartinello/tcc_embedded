import polars as pl
import glob
import os
import time
from datetime import datetime
from query.db_duck import con
import psutil
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from gerar_json import registrar_benchmark_carga
from resource_monitor import ResourceMonitor

base_path = os.path.abspath("../data/logs") 
pattern = os.path.join(base_path, "*_2", "*_new.csv")
files = glob.glob(pattern)

if not files:
    raise FileNotFoundError(f"Nenhum arquivo CSV encontrado no padrão: {pattern}")

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

filtros_exact_lower = [f.lower() for f in FILTROS_EXACT]
filtros_like_lower = [f.lower() for f in FILTROS_LIKE]
accepted_dates_dt = [datetime.strptime(d, "%Y-%m-%d").date() for d in ACCEPTED_DATES]

process = psutil.Process(os.getpid())
monitor = ResourceMonitor(process, interval=0.5)
monitor.start()
start_time = time.perf_counter()
total_rows_inserted = 0

con.execute("DROP TABLE IF EXISTS events_df;")
create_table_sql = """
CREATE TABLE events_df (
    event_timestamp TIMESTAMP, 
    event_type VARCHAR,
    some_id VARCHAR,
    event_system VARCHAR,
    event_description VARCHAR,
    event_id VARCHAR,
    filename VARCHAR,
    uf VARCHAR,
    filename_only VARCHAR,
    city_code VARCHAR,
    zone_code VARCHAR,
    section_code VARCHAR,
    event_date DATE
);
"""
try:
    con.execute(create_table_sql)
    print("Table 'events_df' created successfully.")
except Exception as e:
    print(f"Error creating table 'events_df': {e}")
    monitor.stop()
    monitor.join()
    con.close()
    sys.exit(1)

print(f"Processing {len(files)} files in batches...")
for i, file_path in enumerate(files):
    file_start_time = time.perf_counter()
    try:
        df_temp = pl.read_csv(
            file_path,
            separator="\t",
            encoding="utf8-lossy",
            has_header=False,
            try_parse_dates=False 
        ).with_columns(
            pl.lit(file_path).alias("filename")
        )

        df_transformed = df_temp.rename({
            "column_1": "event_timestamp_str", 
            "column_2": "event_type",
            "column_3": "some_id",
            "column_4": "event_system",
            "column_5": "event_description",
            "column_6": "event_id"
        }).with_columns([
            pl.col("filename").str.extract(r'2_([A-Z]{2})', 1).alias("uf"),
            pl.col("filename").str.extract(r'([^\\/]+)$', 1).alias("filename_only")
        ]).with_columns([
            pl.col("event_timestamp_str").str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M:%S", strict=False).alias("event_timestamp"), 
            pl.col("filename_only").str.slice(8, 5).alias("city_code"),
            pl.col("filename_only").str.slice(13, 4).alias("zone_code"),
            pl.col("filename_only").str.slice(17, 4).alias("section_code"),
        ]).with_columns(
             pl.col("event_timestamp").dt.date().alias("event_date") 
        ).with_columns(
            pl.col("event_description").str.strip_chars().str.to_lowercase()
        )

        like_conditions = []
        for pattern in filtros_like_lower:
            if pattern.startswith('%') and pattern.endswith('%'):
                like_conditions.append(pl.col("event_description").str.contains(pattern.strip('%'), literal=False))
            elif pattern.endswith('%'):
                like_conditions.append(pl.col("event_description").str.starts_with(pattern.rstrip('%')))
            elif pattern.startswith('%'):
                like_conditions.append(pl.col("event_description").str.ends_with(pattern.lstrip('%')))
            else: 
                like_conditions.append(pl.col("event_description") == pattern)
        
        like_filter = pl.any_horizontal(like_conditions) if like_conditions else pl.lit(True)
        exact_filter = pl.col("event_description").is_in(filtros_exact_lower)
        date_filter = pl.col("event_date").is_in(accepted_dates_dt)

        df_filtered_chunk = df_transformed.filter(
            (exact_filter | like_filter) & date_filter 
        )

        print(f"Linhas após filtro: {df_filtered_chunk.shape[0]}")
        print("Conectando ao DuckDB...")
        rows_in_chunk = df_filtered_chunk.height
        if rows_in_chunk > 0:
            con.register("events_df", df_filtered_chunk)
            total_rows_inserted += rows_in_chunk
            file_time = time.perf_counter() - file_start_time
            print(f"Linhas inseridas {rows_in_chunk}. Time: {file_time:.2f}s. Total: {total_rows_inserted}")
        else:
            file_time = time.perf_counter() - file_start_time
            print(f"Nenhuma linha para filtro: {file_time:.2f}s.")

        del df_temp
        del df_transformed
        del df_filtered_chunk

    except Exception as e:
        print(f"\nERRO PARA PROCESSAR {os.path.basename(file_path)}: {e}")

print("\nArquivos processados.")
end_time = time.perf_counter()
monitor.stop()
monitor.join()

tempo_execucao = end_time - start_time
tamanho_total_bytes = sum(os.path.getsize(f) for f in files if os.path.exists(f))
tamanho_total_mb = tamanho_total_bytes / (1024 * 1024)

cpu_percent_medio = monitor.get_average_cpu()
mem_before = monitor.mem_readings[0] if monitor.mem_readings else 0
mem_after = monitor.mem_readings[-1] if monitor.mem_readings else 0
mem_max = monitor.get_max_memory()

print(f"\n--- BENCHMARK RESULTS ---")
print(f"Total rows inserted: {total_rows_inserted}")
print(f"Total files processed: {len(files)}")
print(f"Total data size (CSV): {tamanho_total_mb:.2f} MB")
print(f"Total execution time: {tempo_execucao:.2f} seconds")
print(f"Memory usage: Start={mem_before:.2f} MB | End={mem_after:.2f} MB | Peak={mem_max:.2f} MB")
print(f"Average CPU usage: {cpu_percent_medio:.2f}%" if cpu_percent_medio else "Average CPU usage: N/A")

registrar_benchmark_carga(
    banco="DuckDB",
    tempo_execucao=tempo_execucao,
    linhas=total_rows_inserted,
    arquivos=len(files),
    tamanho_total_mb=tamanho_total_mb,
    mem_before=mem_before,
    mem_after=mem_after,
    cpu_percent=cpu_percent_medio,
    mem_max=mem_max
)

try:
    con.close()
    print("DuckDB connection closed.")
except Exception as e:
    print(f"Error closing DuckDB connection: {e}")
