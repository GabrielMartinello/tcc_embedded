import polars as pl
import glob
import os
import time
from datetime import datetime
from sqlalchemy import create_engine, Table, Column, String, MetaData, DateTime, Date, text, Integer


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
    pl.col("filename_only").str.slice(8, 5).alias("city_code"),   # município
    pl.col("filename_only").str.slice(13, 4).alias("zone_code"),  # zona
    pl.col("filename_only").str.slice(17, 4).alias("section_code")# seção
])

df = df.with_columns(
    pl.col("event_timestamp").dt.date().alias("event_date")
)

df = df.with_columns(
    pl.col("event_description").str.strip_chars().str.to_lowercase()
)

filtros_exact_lower = [f.lower() for f in FILTROS_EXACT]
filtros_like_lower = [f.lower() for f in FILTROS_LIKE]

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
print(df_filtered.schema)
print("Conectando ao SQLite...")

engine = create_engine("sqlite:///banco_bagre.db")
metadata = MetaData()

events_df_table = Table(
    "events_df",
    metadata,
    Column("event_timestamp", DateTime),
    Column("event_type", String),
    Column("some_id", Integer),
    Column("event_system", String),
    Column("event_description", String),
    Column("event_id", String),
    Column("filename", String),
    Column("uf", String),
    Column("filename_only", String),
    Column("city_code", String),
    Column("zone_code", String),
    Column("section_code", String),
    Column("event_date", Date),
)

with engine.begin() as conn:
    metadata.drop_all(conn)
    metadata.create_all(conn)
    conn.execute(text("PRAGMA synchronous = OFF")) 
    conn.execute(text("PRAGMA journal_mode = MEMORY"))
    conn.execute(text("VACUUM;"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_event_description ON events_df (event_description);"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_event_timestamp ON events_df (event_timestamp);"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_some_id ON events_df (some_id);"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_event_system ON events_df (event_system);"))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_key_events_df 
        ON events_df (event_system, some_id, city_code, uf, zone_code, section_code);
    """))

print("Convertendo polars para pandas")
df_filtered_pd = df_filtered.to_pandas()
print("Fim da conversão")
print(df_filtered_pd.dtypes)    

print(f"Inserindo linhas no sqlite")
chunk_size = 100000

for i in range(0, len(df_filtered_pd), chunk_size):
    chunk = df_filtered_pd.iloc[i:i+chunk_size]
    chunk.to_sql(
        "events_df",
        con=engine,
        if_exists="append",
        index=False,
        method=None
   )

with engine.begin() as conn:
    conn.execute(text("VACUUM;"))
    conn.execute(text("ANALYZE;"))    

print("Dados inseridos e índices criados com sucesso!")
end = time.perf_counter()
print(f"\nProcesso concluído em {end - start:.2f} segundos")
