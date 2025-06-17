ANCORA = 'aguardando digitação do identificador do eleitor'
FIM_VOTO = 'o voto do eleitor foi computado'
events_df = "./events_df.parquet"

query_tempo_medio_voto_por_zona=f"""
WITH
  eventos_fim AS (
    SELECT 
      uf,
      city_code,
      zone_code,
      section_code,
      event_system,
      some_id,
      event_timestamp AS tempo_final
    FROM '{events_df}'
    WHERE event_system = 'VOTA' 
      AND event_description = '{FIM_VOTO}'
  ),
  eventos_ancora AS (
    SELECT 
      event_system,
      uf,
      city_code,
      zone_code,
      section_code,
      some_id,
      event_timestamp AS data_inicio
    FROM '{events_df}'
    WHERE event_description = '{ANCORA}'
  ),
  eventos_join AS (
    SELECT 
      f.uf,
      f.city_code,
      f.zone_code,
      f.section_code,
      f.event_system,
      f.some_id,
      f.tempo_final,
      max(a.data_inicio) AS data_inicio
    FROM eventos_fim f
    LEFT JOIN eventos_ancora a
      ON a.event_system = f.event_system
      AND a.uf = f.uf
      AND a.city_code = f.city_code
      AND a.zone_code = f.zone_code
      AND a.section_code = f.section_code
      AND a.some_id = f.some_id
      AND (a.data_inicio < f.tempo_final)
    GROUP BY 
      f.uf,
      f.city_code,
      f.zone_code,
      f.section_code,
      f.event_system,
      f.some_id,
      f.tempo_final
  )
SELECT
  zone_code,
  AVG((toUnixTimestamp(tempo_final) - toUnixTimestamp(data_inicio)) / 60) AS tempo_medio_zona
FROM eventos_join
WHERE data_inicio IS NOT NULL
GROUP BY zone_code
ORDER BY tempo_medio_zona DESC
LIMIT 10
"""

query_top_10_pessoas_que_mais_demoraram=f"""
WITH
  eventos_fim AS (
    SELECT 
      uf,
      city_code,
      zone_code,
      section_code,
      event_system,
      some_id,
      event_timestamp AS tempo_final
    FROM '{events_df}'
    WHERE event_system = 'VOTA' 
      AND event_description = '{FIM_VOTO}'
  ),
  eventos_ancora AS (
    SELECT 
      event_system,
      uf,
      city_code,
      zone_code,
      section_code,
      some_id,
      event_timestamp AS data_inicio
    FROM '{events_df}'
    WHERE event_description = '{ANCORA}'
  ),
  eventos_join AS (
    SELECT 
      f.uf,
      f.city_code,
      f.zone_code,
      f.section_code,
      f.event_system,
      f.some_id,
      f.tempo_final,
      max(a.data_inicio) AS data_inicio
    FROM eventos_fim f
    LEFT JOIN eventos_ancora a
      ON a.event_system = f.event_system
      AND a.uf = f.uf
      AND a.city_code = f.city_code
      AND a.zone_code = f.zone_code
      AND a.section_code = f.section_code
      AND a.some_id = f.some_id
      AND (a.data_inicio < f.tempo_final)
    GROUP BY 
      f.uf,
      f.city_code,
      f.zone_code,
      f.section_code,
      f.event_system,
      f.some_id,
      f.tempo_final
  )
SELECT
  uf,
  city_code,
  zone_code,   
  data_inicio,
  tempo_final,
  (toUnixTimestamp(tempo_final) - toUnixTimestamp(data_inicio)) / 60 AS tempo_voto
FROM eventos_join
WHERE data_inicio IS NOT NULL
order by tempo_voto desc
LIMIT 10
"""

query_tempo_medio_voto_cidade=f"""
WITH
  eventos_fim AS (
    SELECT 
      uf,
      city_code,
      zone_code,
      section_code,
      event_system,
      some_id,
      event_timestamp AS tempo_final
    FROM '{events_df}'
    WHERE event_system = 'VOTA' 
      AND event_description = '{FIM_VOTO}'
  ),
  eventos_ancora AS (
    SELECT 
      event_system,
      uf,
      city_code,
      zone_code,
      section_code,
      some_id,
      event_timestamp AS data_inicio
    FROM '{events_df}'
    WHERE event_description = '{ANCORA}'
  ),
  eventos_join AS (
    SELECT 
      f.uf,
      f.city_code,
      f.zone_code,
      f.section_code,
      f.event_system,
      f.some_id,
      f.tempo_final,
      max(a.data_inicio) AS data_inicio
    FROM eventos_fim f
    LEFT JOIN eventos_ancora a
      ON a.event_system = f.event_system
      AND a.uf = f.uf
      AND a.city_code = f.city_code
      AND a.zone_code = f.zone_code
      AND a.section_code = f.section_code
      AND a.some_id = f.some_id
      AND (a.data_inicio < f.tempo_final)
    GROUP BY 
      f.uf,
      f.city_code,
      f.zone_code,
      f.section_code,
      f.event_system,
      f.some_id,
      f.tempo_final
  )
SELECT
  city_code,
  AVG((toUnixTimestamp(tempo_final) - toUnixTimestamp(data_inicio)) / 60) AS tempo_medio_cidade
FROM eventos_join
WHERE data_inicio IS NOT NULL
GROUP BY city_code
ORDER BY tempo_medio_cidade DESC
LIMIT 10
"""

query_tempo_medio_voto_uf=f"""
WITH
  eventos_fim AS (
    SELECT 
      uf,
      city_code,
      zone_code,
      section_code,
      event_system,
      some_id,
      event_timestamp AS tempo_final
    FROM '{events_df}'
    WHERE event_system = 'VOTA' 
      AND event_description = '{FIM_VOTO}'
  ),
  eventos_ancora AS (
    SELECT 
      event_system,
      uf,
      city_code,
      zone_code,
      section_code,
      some_id,
      event_timestamp AS data_inicio
    FROM '{events_df}'
    WHERE event_description = '{ANCORA}'
  ),
  eventos_join AS (
    SELECT 
      f.uf,
      f.city_code,
      f.zone_code,
      f.section_code,
      f.event_system,
      f.some_id,
      f.tempo_final,
      max(a.data_inicio) AS data_inicio
    FROM eventos_fim f
    LEFT JOIN eventos_ancora a
      ON a.event_system = f.event_system
      AND a.uf = f.uf
      AND a.city_code = f.city_code
      AND a.zone_code = f.zone_code
      AND a.section_code = f.section_code
      AND a.some_id = f.some_id
      AND (a.data_inicio < f.tempo_final)
    GROUP BY 
      f.uf,
      f.city_code,
      f.zone_code,
      f.section_code,
      f.event_system,
      f.some_id,
      f.tempo_final
  )
SELECT
  uf,
  AVG((toUnixTimestamp(tempo_final) - toUnixTimestamp(data_inicio)) / 60) AS tempo_medio_uf
FROM eventos_join
WHERE data_inicio IS NOT NULL
GROUP BY uf
ORDER BY tempo_medio_uf DESC
LIMIT 10
"""

query_count_todos_registros=f"""
SELECT COUNT(*) as QuantidadeRegistros FROM '{events_df}'
"""

query_qtde_registros_eventos_sistema=f"""
 SELECT 
        event_system,
        COUNT(*) AS qtd_linhas
    FROM '{events_df}'
    GROUP BY event_system
"""

query_qtde_erro_alerta_info=f"""SELECT 
        event_type,
        COUNT(*) AS qtd_linhas
    FROM '{events_df}'
    WHERE event_system='VOTA' OR event_system='RED'
    GROUP BY event_type
"""
