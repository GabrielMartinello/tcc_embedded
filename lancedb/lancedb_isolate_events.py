import polars as pl
import glob
import os
import time
from datetime import datetime
import lancedb
import psutil
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from gerar_json import registrar_benchmark_carga
from resource_monitor import ResourceMonitor

# --- CONFIG ---
base_path = os.path.abspath("../data/logs") 
pattern = os.path.join(base_path, "2_*", "*_new.csv")
files = glob.glob(pattern)

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
    'Zona Eleitoral%', 'Seção Eleitoral%', 'Município%', 'Local de Votação%',
    'Turno da UE%', 'Identificação do Modelo de Urna%', 'Urna pronta para receber vot%',
    'Voto confirmado par%', '%Digital%',
    'Tipo de habilitação do eleitor [biométrica]%', 'Solicita digital%'
]

filtros_exact_lower = [f.lower() for f in FILTROS_EXACT]
filtros_like_lower = [f.lower() for f in FILTROS_LIKE]
accepted_dates_dt = [datetime.strptime(d, "%Y-%m-%d").date() for d in ACCEPTED_DATES]

# --- MONITORAMENTO ---
process = psutil.Process(os.getpid())
monitor = ResourceMonitor(process, interval=0.5)
monitor.start()
start_time = time.perf_counter()

# --- INICIALIZAÇÃO LANCEDB ---
lance_path = "./lancedb_events"
db = lancedb.connect(lance_path)

table = None
total_rows_inserted = 0
BUFFER = []
BATCH_SIZE = 100_000

print(f"Processing {len(files)} files...")

for i, file_path in enumerate(files):
    file_start_time = time.perf_counter()
    try:
        df_temp = pl.read_csv(
            file_path,
            separator="\t",
            encoding="utf8-lossy",
            has_header=False,
            try_parse_dates=False,
            low_memory=True
        ).with_columns(
            pl.lit(file_path).alias("filename")
        )

        df_transformed = df_temp.rename({
            "column_1": "event_timestamp", 
            "column_2": "event_type",
            "column_3": "some_id",
            "column_4": "event_system",
            "column_5": "event_description",
            "column_6": "event_id"
        }).with_columns([
            pl.col("filename").str.extract(r'2_([A-Z]{2})', 1).alias("uf"),
            pl.col("filename").str.extract(r'([^\\/]+)$', 1).alias("filename_only")
        ]).with_columns([
            pl.col("event_timestamp").str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M:%S", strict=False),
            pl.col("filename_only").str.slice(8, 5).alias("city_code"),
            pl.col("filename_only").str.slice(13, 4).alias("zone_code"),
            pl.col("filename_only").str.slice(17, 4).alias("section_code"),
        ]).with_columns([
            pl.col("event_timestamp").dt.date().alias("event_date"),
            pl.col("event_description").str.strip_chars().str.to_lowercase()
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

        like_filter = pl.any_horizontal(like_conditions) if like_conditions else pl.lit(True)
        exact_filter = pl.col("event_description").is_in(filtros_exact_lower)
        date_filter = pl.col("event_date").is_in(accepted_dates_dt)

        df_filtered_chunk = df_transformed.filter(
            (exact_filter | like_filter) & date_filter
        )

        rows = df_filtered_chunk.height
        if rows > 0:
            BUFFER.append(df_filtered_chunk)
            total_rows_inserted += rows

            if sum(df.height for df in BUFFER) >= BATCH_SIZE:
                df_batch = pl.concat(BUFFER, how="vertical")
                arrow_batch = df_batch.to_arrow()
                if table is None:
                    table = db.create_table("events_df", data=arrow_batch)
                    print("Tabela 'events_df' criada no LanceDB.")
                else:
                    table.add(arrow_batch)
                BUFFER.clear()

        file_time = time.perf_counter() - file_start_time
        print(f"[{i+1}/{len(files)}] {os.path.basename(file_path)}: {rows} linhas processadas em {file_time:.2f}s")

        del df_temp, df_transformed, df_filtered_chunk

    except Exception as e:
        print(f"\nErro ao processar {os.path.basename(file_path)}: {e}")

# --- FINALIZA O RESTO DO BUFFER ---
if BUFFER:
    df_batch = pl.concat(BUFFER, how="vertical")
    arrow_batch = df_batch.to_arrow()
    if table is None:
        table = db.create_table("events_df", data=arrow_batch)
        print("Tabela 'events_df' criada no LanceDB (último batch).")
    else:
        table.add(arrow_batch)

monitor.stop()
monitor.join()
end_time = time.perf_counter()

# --- BENCHMARK ---
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
    banco="LanceDB",
    tempo_execucao=tempo_execucao,
    linhas=total_rows_inserted,
    arquivos=len(files),
    tamanho_total_mb=tamanho_total_mb,
    mem_before=mem_before,
    mem_after=mem_after,
    cpu_percent=cpu_percent_medio,
    mex_max=mem_max
)
