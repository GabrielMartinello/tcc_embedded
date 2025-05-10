import csv
import sqlite3

csv_file = './VOTOS_POR_UF.csv'

conn = sqlite3.connect('tcc.db')
cur = conn.cursor()

with open(csv_file, mode='r') as file:
    # Criando um objeto reader para ler o CSV
    csv_reader = csv.reader(file)
    
    # Lendo o cabeçalho do CSV (primeira linha)
    columns = next(csv_reader)
    
    # Criando a tabela dinamicamente com base no cabeçalho do CSV
    table_name = 'events'  # Nome da tabela
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_timestamp TEXT,
        event_date TEXT,
        event_type TEXT,
        some_id TEXT,
        event_system TEXT,
        event_description TEXT,
        event_id TEXT,
        filename TEXT,
        city_code TEXT,
        zone_code TEXT,
        section_code TEXT,
        uf TEXT
    )
    """
    cur.execute(create_table_query)

    # Inserir os dados na tabela
    for row in csv_reader:
        placeholders = ', '.join(['?'] * len(columns))
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        cur.execute(insert_query, row)

conn.commit()
conn.close()
