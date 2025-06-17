import lancedb
import polars as pl
import pyarrow
import duckdb

print("Polars version:", pl.__version__)
print("PyArrow version:", pyarrow.__version__)

db = lancedb.connect("./lancedb_events")
table = db.open_table("events_df")

arrow_table = table.to_lance()
ANCORA = 'aguardando digitação do identificador do eleitor'
FIM_VOTO = 'o voto do eleitor foi computado'
query_count_todos_registros = f"""
SELECT * FROM arrow_table limit 10
"""

query_teste = f"""
select *
from (select x.*
                from (SELECT voto.uf,
                            voto.city_code,
                            voto.zone_code,
                            voto.section_code,
                            voto.event_system,
                            voto.some_id as urna_num,
                            voto.event_description as evento,
                            voto.event_timestamp
                        FROM arrow_table voto
                WHERE voto.event_system = 'VOTA'
                and voto.event_description = '{FIM_VOTO}'
                order by voto.uf, voto.city_code, voto.zone_code, voto.section_code, voto.some_id, voto.event_timestamp) x) Y
limit 10
"""

result = duckdb.query(query_teste)

print(result)

