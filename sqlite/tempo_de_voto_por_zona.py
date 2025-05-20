import sqlite3
import pandas as pd
import time

ANCORA = 'Aguardando digitação do identificador do eleitor'
FIM_VOTO = 'O voto do eleitor foi computado'

conn = sqlite3.connect('events.db')

# Função para converter o timestamp no formato correto dentro do SQL
# converte 'DD/MM/YYYY HH:MM:SS' para 'YYYY-MM-DD HH:MM:SS'
def converte_ts(col):
    return (
        "substr({0}, 7, 4) || '-' || "   # YYYY
        "substr({0}, 4, 2) || '-' || "    # MM
        "substr({0}, 1, 2) || ' ' || "    # DD
        "substr({0}, 12, 8)"              # HH:MM:SS
    ).format(col)

query_vote_data_sqlite = f"""
WITH inicio AS (
    SELECT 
        {converte_ts('event_timestamp')} AS inicio_voto,
        uf,
        event_date,
        filename,
        city_code,
        zone_code,
        section_code,
        ROW_NUMBER() OVER (
            PARTITION BY event_date, uf, filename, city_code, zone_code, section_code
            ORDER BY {converte_ts('event_timestamp')}
        ) AS vote_id
    FROM votos_por_uf
    WHERE event_description = '{ANCORA}'
),
fim AS (
    SELECT 
        {converte_ts('event_timestamp')} AS fim_voto,
        uf,
        event_date,
        filename,
        city_code,
        zone_code,
        section_code,
        ROW_NUMBER() OVER (
            PARTITION BY event_date, uf, filename, city_code, zone_code, section_code
            ORDER BY {converte_ts('event_timestamp')}
        ) AS vote_id
    FROM votos_por_uf
    WHERE event_description = '{FIM_VOTO}'
),
votos_pareados AS (
    SELECT 
        i.uf,
        i.event_date,
        i.zone_code,
        i.section_code,
        i.vote_id,
        i.inicio_voto,
        f.fim_voto,
        i.city_code,
        (julianday(f.fim_voto) - julianday(i.inicio_voto)) * 86400.0 AS duracao_segundos
    FROM inicio i
    JOIN fim f
        ON i.uf = f.uf 
        AND i.event_date = f.event_date
        AND i.filename = f.filename
        AND i.city_code = f.city_code
        AND i.zone_code = f.zone_code
        AND i.section_code = f.section_code
        AND i.vote_id = f.vote_id
    WHERE f.fim_voto > i.inicio_voto
)
SELECT 
    uf,
    zone_code,
    section_code,
    city_code,
    AVG(duracao_segundos) AS tempo_medio_voto_segundos,
    COUNT(*) AS total_votos
FROM votos_pareados
GROUP BY uf, zone_code, section_code, city_code
ORDER BY tempo_medio_voto_segundos DESC
LIMIT 10
"""

start = time.time()
df = pd.read_sql_query(query_vote_data_sqlite, conn)
end = time.time()

print(f"A consulta demorou {end - start:.2f} segundos")
print(df)

conn.close()
