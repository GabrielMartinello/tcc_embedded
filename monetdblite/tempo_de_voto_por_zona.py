import monetdblite
import time

DB_PATH = './events_monet'
ANCORA = 'Aguardando digitação do identificador do eleitor'
FIM_VOTO = 'O voto do eleitor foi computado'

monetdblite.init(DB_PATH)
con = monetdblite.connect()

query = f"""SELECT event_timestamp,
       str_to_timestamp(event_timestamp, '%d/%m/%Y %H:%M:%S') AS FORMATED
FROM votos_por_uf
WHERE event_description IN ('{ANCORA}', '{FIM_VOTO}')
LIMIT 10;"""

query_vote_data= f""" 
WITH inicio AS (
    SELECT
        CAST(str_to_timestamp(event_timestamp, '%d/%m/%Y %H:%M:%S') AS TIMESTAMP) AS inicio_voto,
        uf,
        event_date,
        filename,
        zone_code,
        ROW_NUMBER() OVER (
            PARTITION BY event_date, uf, filename, zone_code
            ORDER BY CAST(str_to_timestamp(event_timestamp, '%d/%m/%Y %H:%M:%S') AS TIMESTAMP)
        ) AS vote_id
    FROM votos_por_uf
    WHERE event_description = '{ANCORA}'
),
fim AS (
    SELECT
        CAST(str_to_timestamp(event_timestamp, '%d/%m/%Y %H:%M:%S') AS TIMESTAMP) AS fim_voto,
        uf,
        event_date,
        filename,
        zone_code,
        ROW_NUMBER() OVER (
            PARTITION BY event_date, uf, filename, zone_code
            ORDER BY CAST(str_to_timestamp(event_timestamp, '%d/%m/%Y %H:%M:%S') AS TIMESTAMP)
        ) AS vote_id
    FROM votos_por_uf
    WHERE event_description = '{FIM_VOTO}'
),
votos_completos AS (
    SELECT 
        i.uf,
        i.zone_code,
        i.event_date,
        i.vote_id,
        i.inicio_voto,
        f.fim_voto,
        CAST(
            EXTRACT(SECOND FROM (f.fim_voto - i.inicio_voto)) AS DOUBLE
        ) AS duracao_segundos
    FROM inicio i
    JOIN fim f ON 
        i.vote_id = f.vote_id AND 
        i.uf = f.uf AND 
        i.event_date = f.event_date AND 
        i.filename = f.filename AND
        i.zone_code = f.zone_code
    WHERE f.fim_voto > i.inicio_voto         
)
SELECT 
    uf,
    zone_code,
    AVG(duracao_segundos) AS tempo_medio_voto_segundos,
    COUNT(*) AS total_votos
FROM votos_completos
GROUP BY uf, zone_code
ORDER BY tempo_medio_voto_segundos DESC
"""

start = time.perf_counter()
result = monetdblite.sql(query_vote_data, client=con)
end = time.perf_counter()

print(f"Consulta executada em {end - start:.2f} segundos")
print(result)
