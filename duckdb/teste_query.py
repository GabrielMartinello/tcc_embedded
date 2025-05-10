import duckdb

tabela = "./ALL_UFS.csv"
query = F"""
    SELECT 
        COUNT(*) as numeroRegistros
    FROM 
        read_csv_auto('{tabela}')
"""
con = duckdb.connect()
result = con.execute(query).fetchdf()
print(result)