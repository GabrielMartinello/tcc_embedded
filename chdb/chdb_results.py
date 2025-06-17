import chdb
import queries


conn = chdb.connect()

parquet_path = "./events_df.parquet"
conn.query("SET allow_experimental_join_condition = 1;")
query_sql = f"SELECT toUnixTimestamp(event_timestamp) FROM '{parquet_path}' limit 10"

result = conn.query(queries.query_tempo_medio_voto_por_zona)
print(result)