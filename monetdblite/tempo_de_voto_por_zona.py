import monetdblite
import time
import numpy as np
import pandas as pd  # Importa pandas para formatação

# Formatando para visualizar melhor no console

DB_PATH = './events_monet'
ANCORA = 'Aguardando digitação do identificador do eleitor'
FIM_VOTO = 'O voto do eleitor foi computado'

monetdblite.init(DB_PATH)
con = monetdblite.connect()

query = f"""SELECT 
  EXTRACT(DAY FROM diff_interval) * 86400 +
  EXTRACT(HOUR FROM diff_interval) * 3600 +
  EXTRACT(MINUTE FROM diff_interval) * 60 +
  EXTRACT(SECOND FROM diff_interval) AS total_seconds
FROM (
  SELECT (TIMESTAMP '2024-10-27 16:33:51' - TIMESTAMP '2024-10-27 16:30:00') AS diff_interval
) AS sub;

"""

query_vote_data= f""" 
WITH inicio AS (
    SELECT
        *,
        str_to_timestamp(event_timestamp, '%d/%m/%Y %H:%M:%S') AS inicio_voto,
        ROW_NUMBER() OVER (
            PARTITION BY event_date, uf, filename
            ORDER BY str_to_timestamp(event_timestamp, '%d/%m/%Y %H:%M:%S')
        ) AS vote_id
    FROM votos_por_uf
    WHERE event_description = '{ANCORA}'
),
fim AS (
    SELECT
        str_to_timestamp(event_timestamp, '%d/%m/%Y %H:%M:%S') AS fim_voto,
        uf,
        event_date,
        filename,
        ROW_NUMBER() OVER (
            PARTITION BY event_date, uf, filename
            ORDER BY str_to_timestamp(event_timestamp, '%d/%m/%Y %H:%M:%S') 
        ) AS vote_id
    FROM votos_por_uf
    WHERE event_description = '{FIM_VOTO}'
),
votos_completos AS (
    SELECT 
        i.uf,
        i.zone_code,
        i.section_code,
        i.event_date,
        i.vote_id,
        i.inicio_voto,
        f.fim_voto,
        i.city_code,
        EXTRACT(DAY FROM (f.fim_voto - i.inicio_voto)) * 86400 +
        EXTRACT(HOUR FROM (f.fim_voto - i.inicio_voto)) * 3600 +
        EXTRACT(MINUTE FROM (f.fim_voto - i.inicio_voto)) * 60 +
        EXTRACT(SECOND FROM (f.fim_voto - i.inicio_voto)) AS duracao_segundos
    FROM inicio i
    JOIN fim f ON 
        i.vote_id = f.vote_id AND 
        i.uf = f.uf AND 
        i.event_date = f.event_date AND 
        i.filename = f.filename
    WHERE f.fim_voto > i.inicio_voto         
)
SELECT 
    uf,
    zone_code,
    section_code,
    city_code,
    AVG(duracao_segundos) AS tempo_medio_voto_segundos,
    COUNT(*) AS total_votos
FROM votos_completos
GROUP BY uf, zone_code, section_code, city_code
ORDER BY tempo_medio_voto_segundos DESC
LIMIT 10
"""

start = time.perf_counter()
result = monetdblite.sql(query_vote_data, client=con)
end = time.perf_counter()

print(f"Consulta executada em {end - start:.2f} segundos")
data = {k: v.filled() if isinstance(v, np.ma.MaskedArray) else v for k, v in result.items()}
df = pd.DataFrame(data)
df['tempo_medio_voto_segundos'] = df['tempo_medio_voto_segundos'].round(2)
print(df)
