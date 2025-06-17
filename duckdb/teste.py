import duckdb
from query import queries

con = duckdb.connect("banco_bagre.duckdb")
result = con.execute(queries.query_tempo_medio_voto_por_zona).fetchdf()
print(result)