import duckdb
import pandas as pd
import time

csv_path = './VOTOS_POR_UF.csv'

cursor = duckdb.connect()

#utilizei isso pra calcular o tempo médio de votação por zona eleitoral com base na diferença entre os eventos:
#Ou seja, a constante ANCORA é para o início do voto
ANCORA = 'Aguardando digitação do identificador do eleitor'
#E aqui é pra o fim do voto, preciso disso pra pegar a data e subtrair as diferenças deles
FIM_VOTO = 'O voto do eleitor foi computado'

# Cria uma tabela temporaria chamada base
# Soma 1 sempre que aparece o evento de início
# Agrupa por event_date, uf e filename (evita mistura de arquivos ou dias diferentes)
# Ordena por event_timestamp (ordem dos eventos)
# Como não há um identificador de eleitor direto, o vote_id é criado com base 
# no número de vezes que o evento de início de voto aparece 
# cada nova ocorrência representa um novo voto.
# Calcula o tempo médio por zonas
query_vote_data = f"""
WITH dados AS (
    SELECT 
        *,
        CAST(event_timestamp AS TIMESTAMP) AS ts
    FROM read_csv_auto('{csv_path}')
    WHERE event_description IN ('{ANCORA}', '{FIM_VOTO}')
),
inicio AS (
    SELECT 
        ts AS inicio_voto,
        uf,
        event_date,
        filename,
        city_code,
        zone_code,
        section_code,
        ROW_NUMBER() OVER (
            PARTITION BY event_date, uf, filename, city_code, zone_code, section_code
            ORDER BY ts
        ) AS vote_id
    FROM dados
    WHERE event_description = '{ANCORA}'
),
fim AS (
    SELECT 
        ts AS fim_voto,
        uf,
        event_date,
        filename,
        city_code,
        zone_code,
        section_code,
        ROW_NUMBER() OVER (
            PARTITION BY event_date, uf, filename, city_code, zone_code, section_code
            ORDER BY ts
        ) AS vote_id
    FROM dados
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
        EXTRACT(EPOCH FROM f.fim_voto - i.inicio_voto) AS duracao_segundos
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

tic=time.time()
top_zonas = cursor.execute(query_vote_data).fetchdf()
toc=time.time()
print(F"A consulta demorou {toc-tic} segundos")
print(top_zonas)
