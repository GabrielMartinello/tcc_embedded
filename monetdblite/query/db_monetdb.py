import monetdblite

# Cria a conexão uma única vez
DB_PATH = '../events_monet'
monetdblite.init(DB_PATH)
conn = monetdblite.connect()