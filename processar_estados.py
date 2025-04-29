import duckdb
import time
import os

STATES = [
    'AC', 'AL', 'AP', 'AM',
    'BA', 'CE', 'ES', 'GO',
    'MA', 'MT', 'MS', 'MG',
    'PA', 'PB', 'PR', 'PE',
    'PI', 'RJ', 'RN', 'RS',
    'RO', 'RR', 'SC', 'SP',
    'SE', 'TO'
]

if __name__ == "__main__":

    for uf in STATES:
        print(f"Processando estado {uf}...")
        tic = time.time()

        conn = duckdb.connect()
        csv_path = os.path.abspath(f"data/logs/2_{uf}/*_new.csv")

        query = f"""
            COPY (
                SELECT * 
                FROM read_csv('{csv_path}', filename=True, encoding='utf-8')
            )
            TO 'logs_{uf}.csv' (FORMAT CSV, HEADER);
        """
        
        try:
            conn.execute(query)
            toc = time.time()
            print(f"Estado {uf} processado em {toc - tic:.2f} segundos\n")
        except Exception as e:
            print(f"Erro ao processar {uf}: {e}\n")
