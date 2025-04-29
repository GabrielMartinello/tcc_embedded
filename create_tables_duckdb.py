import duckdb
import time
import sys

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
    # Pega o primeiro argumento (estado)
    uf = sys.argv[1]

    if uf not in STATES:
        print("Invalid state")
        sys.exit(1)

    tic = time.time()
    cursor = duckdb.connect(f"{uf}.duckdb")  # Cria um arquivo DuckDB por estado

    # Cria uma tabela no DuckDB carregando todos os CSVs do estado
    query = f"""
        CREATE OR REPLACE TABLE logs_{uf} AS
        SELECT 
            * 
        FROM read_csv('/data/logs/2_{uf}/*.csv', filename=True);
    """
    
    cursor.execute(query)
    toc = time.time()
    print(f"Tempo para a carga do estado: {uf} para o DuckDB: {toc - tic} segundos")
