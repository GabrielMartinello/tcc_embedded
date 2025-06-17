import polars as pl
import glob
import os
import time
from datetime import datetime
import psutil
import sys
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from gerar_json import registrar_benchmark_carga
from resource_monitor import ResourceMonitor

import chdb
print("AAAAAAAAAAAAAA")
print(dir(chdb))

output_path = "./events_df.parquet"

sql_export = f"""
SELECT *
FROM events_df
INTO OUTFILE '{output_path}'
FORMAT Parquet
"""

base_path = os.path.abspath("../data/logs") 
pattern = os.path.join(base_path, "2_*", "*_new.csv")
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

# Inicializa conn CHDB embedded
#conn = get_conn(embedded=True)
conn = chdb.connect()

# Cria tabela (ajuste tipos para ClickHouse)
create_table_sql = """
CREATE TABLE IF NOT EXISTS events_df (
    event_timestamp DateTime64(3), 
    event_type String,
    some_id String,
    event_system String,
    event_description String,
    event_id String,
    filename String,
    uf String,
    filename_only String,
    city_code String,
    zone_code String,
    section_code String,
    event_date Date
) ENGINE = MergeTree()
ORDER BY event_timestamp
"""

def insert_dataframe_clickhouse(conn, table_name, df, chunk_size=50000):
    import pandas as pd

    def format_value(v):
        if pd.isna(v):
            return 'NULL'
        elif isinstance(v, str):
            escaped = v.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(v, (int, float)):
            return str(v)
        elif isinstance(v, pd.Timestamp):
            return f"'{v.strftime('%Y-%m-%d %H:%M:%S')}'"
        else:
            # Fallback para outros tipos
            escaped = str(v).replace("'", "''")
            return f"'{escaped}'"

    values_list = []
    for row in df.itertuples(index=False, name=None):
        formatted_row = ", ".join(format_value(v) for v in row)
        values_list.append(f"({formatted_row})")

    for i in range(0, len(values_list), chunk_size):
        chunk = values_list[i:i+chunk_size]
        sql = f"INSERT INTO {table_name} VALUES " + ", ".join(chunk)
        conn.query(sql)


try:
    conn.query("DROP TABLE IF EXISTS events_df")
    conn.query(create_table_sql)
    print("Tabela 'events_df' criada com sucesso.")
except Exception as e:
    print(f"Erro ao criar tabela: {e}")
    monitor.stop()
    monitor.join()
    sys.exit(1)

print(f"Processando {len(files)} arquivos...")

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

        linhas_chunk = df_filtered_chunk.height
        if linhas_chunk > 0:
            df_to_insert = df_filtered_chunk.to_pandas()
            insert_dataframe_clickhouse(conn, "events_df", df_to_insert, chunk_size=50000)
            total_rows_inserted += linhas_chunk
            file_time = time.perf_counter() - file_start_time
            print(f"Arquivo {i+1}/{len(files)}: Inseridas {linhas_chunk} linhas. Tempo: {file_time:.2f}s. Total inserido: {total_rows_inserted}")
        else:
            file_time = time.perf_counter() - file_start_time
            print(f"Arquivo {i+1}/{len(files)}: Nenhuma linha após filtro. Tempo: {file_time:.2f}s.")

        del df_temp, df_transformed, df_filtered_chunk

    except Exception as e:
        print(f"Erro ao processar arquivo {os.path.basename(file_path)}: {e}")

print("\nProcessamento concluído.")
print("Iniciado export para .parquet")
conn.query(sql_export)
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

print(f"\n--- RESULTADOS BENCHMARK ---")
print(f"Total linhas inseridas: {total_rows_inserted}")
print(f"Total arquivos processados: {len(files)}")
print(f"Tamanho total dos CSVs: {tamanho_total_mb:.2f} MB")
print(f"Tempo total de execução: {tempo_execucao:.2f} segundos")
print(f"Uso de memória: Início={mem_before:.2f} MB | Fim={mem_after:.2f} MB | Pico={mem_max:.2f} MB")
print(f"Uso médio de CPU: {cpu_percent_medio:.2f}%" if cpu_percent_medio else "Uso médio de CPU: N/A")

registrar_benchmark_carga(
    banco="CHDB Embedded",
    tempo_execucao=tempo_execucao,
    linhas=total_rows_inserted,
    arquivos=len(files),
    tamanho_total_mb=tamanho_total_mb,
    mem_before=mem_before,
    mem_after=mem_after,
    cpu_percent=cpu_percent_medio,
    mex_max=mem_max
)

try:
    conn.close()
    print("Conexão CHDB encerrada.")
except Exception as e:
    print(f"Erro ao fechar conexão CHDB: {e}")
