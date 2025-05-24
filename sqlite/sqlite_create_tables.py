import csv
import sqlite3
import os
import time
from datetime import datetime

STATES = [
    'AC', 'AL', 'AP', 'AM',
    'BA', 'CE', 'ES', 'GO',
    'MA', 'MT', 'MS', 'MG',
    'PA', 'PB', 'PR', 'PE',
    'PI', 'RJ', 'RN', 'RS',
    'RO', 'RR', 'SC', 'SP',
    'SE', 'TO'
]

inicio = time.perf_counter()
conn = sqlite3.connect('events.db')
cur = conn.cursor()

cur.execute('''
    CREATE TABLE IF NOT EXISTS events (
        event_timestamp DATETIME,
        event_type TEXT,
        some_id TEXT,
        event_system TEXT,
        event_description TEXT,
        event_id TEXT ,
        filename TEXT
    )
''')
conn.commit()

for uf in STATES:
    folder_path = f"../data/logs/2_{uf}"
    if os.path.exists(folder_path):
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv'):
                csv_file = os.path.join(folder_path, filename)
                print(f"Inserindo dados de {csv_file}...")

                with open(csv_file, mode='r', encoding='utf-8') as f:
                    reader = csv.reader(f, delimiter='\t')  # <-- Aqui é TAB!!!
                    rows = []
                    for row in reader:
                        if len(row) != 6:
                            print(f"Linha inválida no arquivo {filename}: {row}")
                            continue

                        try:
                            #print(row)
                            dt = datetime.strptime(row[0].strip(), '%d/%m/%Y %H:%M:%S')
                            row[0] = dt.strftime('%Y-%m-%d %H:%M:%S')
                            #print(row[0])
                        except Exception as e:
                            print(f"Erro na conversão de timestamp '{row[0]}' no arquivo {filename}: {e}")
                            continue
                       
                        row.append(filename)  # Adiciona o nome do arquivo
                        rows.append(row)

                    placeholders = ', '.join('?' for _ in range(7))  # 6 + 1
                    cur.executemany(f"INSERT INTO events VALUES ({placeholders})", rows)

fim = time.perf_counter()
print(f"Importação concluída em {fim - inicio:.2f} segundos.")

conn.commit()
conn.close()
