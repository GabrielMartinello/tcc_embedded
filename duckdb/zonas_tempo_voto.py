import duckdb
import pandas as pd

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
    WITH base AS (
        SELECT *,
            SUM(CASE WHEN event_description = '{ANCORA}' THEN 1 ELSE 0 END) 
            OVER (PARTITION BY event_date, uf, filename ORDER BY event_timestamp) AS vote_id
        FROM read_csv_auto('{csv_path}')
    ),
    votos_filtrados AS (
        SELECT
            event_timestamp,
            event_description,
            uf,
            event_date,
            filename,
            city_code,
            zone_code,
            section_code,
            vote_id
        FROM base
        WHERE event_description IN ('{ANCORA}', '{FIM_VOTO}')
    ),
    votos_processados AS (
        SELECT
            uf,
            event_date,
            zone_code,
            vote_id,
            MIN(CASE WHEN event_description = '{ANCORA}' THEN event_timestamp END) AS inicio_voto,
            MAX(CASE WHEN event_description = '{FIM_VOTO}' THEN event_timestamp END) AS fim_voto
        FROM votos_filtrados
        GROUP BY uf, event_date, zone_code, vote_id
        HAVING inicio_voto IS NOT NULL AND fim_voto IS NOT NULL AND fim_voto > inicio_voto
    )
    SELECT
        uf,
        zone_code,
        AVG(EXTRACT(epoch FROM fim_voto - inicio_voto)) AS tempo_medio_voto_segundos,
        COUNT(*) AS total_votos
    FROM votos_processados
    GROUP BY uf, zone_code
    ORDER BY tempo_medio_voto_segundos DESC
    LIMIT 10    
"""

top_zonas = cursor.execute(query_vote_data).fetchdf()
print(top_zonas)
