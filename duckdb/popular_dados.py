import duckdb
import time
import os
import glob

if __name__ == "__main__":
    inicio = time.perf_counter()

    conn = duckdb.connect()

    base_path = os.path.abspath("../data/logs")
    pattern = os.path.join(base_path, "2_*", "*_new.csv")
    files = glob.glob(pattern)

    if not files:
        print("Nenhum arquivo encontrado!")
    else:
        # Prepara lista de arquivos para o DuckDB
        files_list = ', '.join(f"'{f}'" for f in files)

        #Pra ficar mais fácil de ver criei um .csv novo
        query = f"""
            COPY (
                SELECT * 
                FROM read_csv([{files_list}], filename=True, encoding='utf-8', delim='\t')
            ) 
            TO 'ALL_UFS.csv' (FORMAT CSV, HEADER);
        """

        conn.execute(query)
        conn.close()

        fim = time.perf_counter()
        print(f"\nProcessamento completo em {fim - inicio:.2f} segundos")
