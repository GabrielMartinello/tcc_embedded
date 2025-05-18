import os
import csv
import glob
import time
import monetdblite

DB_PATH = './events_monet'
DATA_PATH = '../data/logs'

def create_table_if_needed(conn):
    try:
        monetdblite.sql('''
            CREATE TABLE events (
                event_timestamp VARCHAR(50),
                event_type VARCHAR(100),
                some_id VARCHAR(50),
                event_system VARCHAR(100),
                event_description VARCHAR(1000),
                event_id VARCHAR(50),
                filename VARCHAR(255)
            );
        ''', client=conn)
        print("Tabela 'events' criada com sucesso.")
    except monetdblite.exceptions.DatabaseError as e:
        if "name 'events' already in use" in str(e):
            print("Tabela 'events' já existe. Prosseguindo...")
        else:
            raise
# Criei para agrupar linhas e executar várias de uma vez só (muito mais rápido).
# Se for fazer do jeito tradicional, que é um insert por linha fica muito lento no monetdblite
# Com batch, você acumula várias linhas em memória, depois gera um único INSERT com várias linhas
# Reduzindo o número de instruções SQL enviadas
def insert_batch(conn, rows):
    if not rows:
        return
    values = ",\n".join([
        "('{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(
            r[0].replace("'", "''"),
            r[1].replace("'", "''"),
            r[2].replace("'", "''"),
            r[3].replace("'", "''"),
            r[4].replace("'", "''"),
            r[5].replace("'", "''"),
            r[6].replace("'", "''")
        )
        for r in rows
    ])
    sql = f"INSERT INTO events VALUES {values};"
    monetdblite.sql(sql, client=conn)


def main():
    start = time.perf_counter()

    monetdblite.init(DB_PATH)
    conn = monetdblite.connect()

    create_table_if_needed(conn)

    files = glob.glob(os.path.join(DATA_PATH, "2_*", "*.csv"))
    total_rows = 0
    batch = []
    BATCH_SIZE = 500  # Tamanho ideal pode ser ajustado

    for csv_file in files:
        print(f"Inserindo dados de {csv_file}...")
        with open(csv_file, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                if len(row) != 6:
                    print(f"Linha inválida no arquivo {csv_file}: {row}")
                    continue
                row.append(os.path.basename(csv_file))
                batch.append(row)
                total_rows += 1

                if len(batch) >= BATCH_SIZE:
                    insert_batch(conn, batch)
                    batch = []

    if batch:
        insert_batch(conn, batch)

    end = time.perf_counter()
    print(f"\nImportação de {total_rows} linhas concluída em {end - start:.2f} segundos.")
    del conn

if __name__ == '__main__':
    main()
