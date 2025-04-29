import subprocess
import time
import duckdb

if __name__ == "__main__":
    inicio_total = time.time()

    conn = duckdb.connect()
    conn.execute("""
        COPY (
            SELECT * 
            FROM read_csv('logs_*.csv', filename='source_file')
        ) TO 'ALL_UFS.csv' (FORMAT CSV, HEADER);
    """)
    conn.close()

    fim_total = time.time()
    print(f"\nProcessamento completo em {fim_total - inicio_total:.2f} segundos")
