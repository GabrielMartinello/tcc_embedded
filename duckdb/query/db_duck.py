import duckdb

# Cria a conexão uma única vez
con = duckdb.connect("banco_bagre.duckdb")
con.execute("PRAGMA memory_limit='4GB';")