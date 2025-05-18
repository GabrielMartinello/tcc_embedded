import os
import csv
import monetdblite
import time

STATES = [
    'AC', 'AL', 'AP', 'AM',
    'BA', 'CE', 'ES', 'GO',
    'MA', 'MT', 'MS', 'MG',
    'PA', 'PB', 'PR', 'PE',
    'PI', 'RJ', 'RN', 'RS',
    'RO', 'RR', 'SC', 'SP',
    'SE', 'TO'
]

# Inicializa ou conecta ao banco de dados
inicio = time.perf_counter()
monetdblite.init('./events_monet')
conn = monetdblite.connect()

# Tenta criar a tabela (MonetDBLite não suporta IF NOT EXISTS)
try:
    monetdblite.sql(
        'CREATE TABLE events ('
        '"event_timestamp" VARCHAR(50), '
        '"event_type" VARCHAR(100), '
        '"some_id" VARCHAR(50), '
        '"event_system" VARCHAR(100), '
        '"event_description" VARCHAR(1000), '
        '"event_id" VARCHAR(50), '
        '"filename" VARCHAR(255)'
        ');',
        client=conn
    )
    print("Tabela 'events' criada com sucesso.")
except monetdblite.exceptions.DatabaseError as e:
    if "name 'events' already in use" in str(e):
        print("Tabela 'events' já existe. Prosseguindo...")
    else:
        raise

def escape(value):
    return value.replace("'", "''")  # Escapa aspas simples para SQL seguro

def insert_row(row):
    sql = f"""
        INSERT INTO events VALUES (
            '{escape(row[0])}',
            '{escape(row[1])}',
            '{escape(row[2])}',
            '{escape(row[3])}',
            '{escape(row[4])}',
            '{escape(row[5])}',
            '{escape(row[6])}'
        );
    """
    monetdblite.sql(sql, client=conn)

# Leitura dos arquivos e inserção dos dados
for uf in STATES:
    folder_path = f"../data/logs/2_{uf}"
    if os.path.exists(folder_path):
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv'):
                csv_file = os.path.join(folder_path, filename)
                print(f"Inserindo dados de {csv_file}...")

                with open(csv_file, mode='r', encoding='utf-8') as f:
                    reader = csv.reader(f, delimiter='\t')
                    for row in reader:
                        if len(row) != 6:
                            print(f"Linha inválida no arquivo {filename}: {row}")
                            continue
                        row.append(filename)
                        insert_row(row)

fim = time.perf_counter()

print(f"Importação concluída em {fim - inicio:.2f} segundos.")
del conn
