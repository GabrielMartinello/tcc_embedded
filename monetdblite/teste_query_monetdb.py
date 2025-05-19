import monetdblite

DB_PATH = './events_monet'

monetdblite.init(DB_PATH)
con = monetdblite.connect()


query =  "SELECT COUNT(*) FROM votos_por_uf"

result = monetdblite.sql(query, client=con)

print(result   )
