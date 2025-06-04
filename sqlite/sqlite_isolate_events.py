import monetdblite
import time
import os
import sys
import polars as pl
import glob
import os
import time
from datetime import datetime, date
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from gerar_json import registrar_benchmark_carga
from resource_monitor import ResourceMonitor
import psutil
from query.db_sqlite import get_conn
import tempfile 

base_path = os.path.abspath("../data/logs")
pattern = os.path.join(base_path, "*_2", "*_new.csv")
files = glob.glob(pattern)

conn = get_conn()
cursor = conn.cursor()

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

try:
    cursor.execute("DROP TABLE IF EXISTS events_df;")
except Exception as drop_err:
    if "no such table" not in str(drop_err).lower():
         print(f"Tabela nao existe: {drop_err}")

create_table_sql = """
CREATE TABLE events_df (
    event_timestamp DATETIME, 
    event_type VARCHAR(255),
    some_id VARCHAR(255),
    event_system VARCHAR(255),
    event_description VARCHAR(1024),
    event_id VARCHAR(255),
    filename VARCHAR(1024),
    uf VARCHAR(2),
    filename_only VARCHAR(255),
    city_code VARCHAR(10),
    zone_code VARCHAR(10),
    section_code VARCHAR(10),
    event_date DATE
);
"""

try:
    cursor.execute(create_table_sql)
    conn.commit()
    print("Tabela 'events_df' criada com sucesso.")
except Exception as e:
    print(f"Erro ao criar tabela events_df': {e}")
    monitor.stop()
    monitor.join()
    sys.exit(1)
    
def format_sql_literal(value):
    """Formats a Python value into an SQL literal string for SQLite."""
    if value is None:
        return "NULL"
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        escaped_value = str(value).replace("'", "''")  
        return f"'{escaped_value}'"
    
print(f"Processing {len(files)} files in batches...")

insert_sql = """
INSERT INTO events_df (
    event_timestamp, event_type, some_id, event_system, 
    event_description, event_id, filename, uf, filename_only, 
    city_code, zone_code, section_code, event_date
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

# Aqui da pra aumentar, só 500 tá lentasso 
multi_value_batch_size = 50000 
for i, file_path in enumerate(files):
    file_start_time = time.perf_counter()
    print(f"Processing file {i+1}/{len(files)}: {os.path.basename(file_path)}...", end=' ')
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
        ).select([ 
            "event_timestamp", "event_type", "some_id", "event_system", 
            "event_description", "event_id", "filename", "uf", "filename_only", 
            "city_code", "zone_code", "section_code", "event_date"
        ]) 

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
        
        like_filter = pl.any_horizontal(like_conditions) if like_conditions else pl.lit(False)
        exact_filter = pl.col("event_description").is_in(filtros_exact_lower)
        date_filter = pl.col("event_date").is_in(accepted_dates_dt) 

        df_filtered_chunk = df_transformed.filter(
            (exact_filter | like_filter) & date_filter 
        )

        rows_in_chunk = df_filtered_chunk.height
        if rows_in_chunk > 0:
            data_tuples = df_filtered_chunk.rows()
            num_batches = (rows_in_chunk + multi_value_batch_size - 1) // multi_value_batch_size
            
            cursor.executemany(insert_sql, data_tuples)
            conn.commit()
            total_rows_inserted += rows_in_chunk
            file_time = time.perf_counter() - file_start_time
            print(f"Inserted {rows_in_chunk} rows ({num_batches} batches). Time: {file_time:.2f}s. Total: {total_rows_inserted}")
        else:
            file_time = time.perf_counter() - file_start_time
            print(f"No rows to insert after filtering. Time: {file_time:.2f}s.")

        del df_temp
        del df_transformed
        del df_filtered_chunk

    except Exception as e:
        print(f"\nERROR processing file {os.path.basename(file_path)}: {e}")

print("\nAll files processed.")
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
    banco="SQLite", 
    tempo_execucao=tempo_execucao,
    linhas=total_rows_inserted,
    arquivos=len(files),
    tamanho_total_mb=tamanho_total_mb,
    mem_before=mem_before,
    mem_after=mem_after,
    cpu_percent=cpu_percent_medio,
    mem_max=mem_max
)
