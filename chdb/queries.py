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

query_top_10_pessoas_que_mais_demoraram=f"""select y.uf,
       y.city_code,
       y.zone_code,
       y.urna,
       (y.tempo_final - julian(y.data_inicio)) * 1440 as tempo_voto,
       y.data_inicio,
       y.event_timestamp as data_final
from (select x.*,
             (select max(vt.event_timestamp)
              from events_df vt
              where vt.event_system = x.event_system
                and vt.event_description = '{ANCORA}'
                and vt.uf = x.uf
                and vt.city_code = x.city_code
                and vt.zone_code = x.zone_code
                and vt.section_code = x.section_code
                and vt.some_id = x.urna_num
                and julian(vt.event_timestamp) < x.tempo_final) as data_inicio
      from (SELECT voto.uf,
                   voto.city_code,
                   voto.zone_code,
                   voto.section_code,
                   voto.event_system,
                   voto.some_id as urna_num,
                   julian(voto.event_timestamp) as tempo_final,
                   voto.event_description as evento,
                   (SELECT GROUP_CONCAT(event_description) as urna_info
                    FROM events_df urna
                    WHERE urna.event_system = 'GAP'
                      and urna.some_id = voto.some_id
                      and urna.city_code = voto.city_code
                      and urna.uf = voto.uf
                      and urna.zone_code = voto.zone_code
                      and urna.section_code = voto.section_code) as urna,
                   voto.event_timestamp
            FROM events_df voto
            WHERE voto.event_system = 'VOTA'
              and voto.event_description = '{FIM_VOTO}'
            order by voto.uf, voto.city_code, voto.zone_code, voto.section_code, voto.some_id, voto.event_timestamp) x) Y
order by tempo_voto desc
limit 10;
"""

query_tempo_medio_voto_cidade=f"""
select z.city_code,
    avg(z.tempo_voto) as tempo_cidade
from (select y.uf,
            y.city_code,
            y.zone_code,
            y.section_code,
            y.urna_num,
            y.urna,
            y.evento,
            (y.tempo_final - julian(y.data_inicio)) * 1440 as tempo_voto,
            y.data_inicio,
            y.event_timestamp as data_final
        from (select x.*,
                    (select max(vt.event_timestamp)
                        from events_df vt
                        where vt.event_system = x.event_system
                        and vt.event_description = '{ANCORA}'
                        and vt.uf = x.uf
                        and vt.city_code = x.city_code
                        and vt.zone_code = x.zone_code
                        and vt.section_code = x.section_code
                        and vt.some_id = x.urna_num
                        and julian(vt.event_timestamp) < x.tempo_final) as data_inicio
                from (SELECT voto.uf,
                            voto.city_code,
                            voto.zone_code,
                            voto.section_code,
                            voto.event_system,
                            voto.some_id as urna_num,
                            julian(voto.event_timestamp) as tempo_final,
                            voto.event_description as evento,
                            (SELECT GROUP_CONCAT(event_description) as urna_info
                                FROM events_df urna
                                WHERE urna.event_system = 'GAP'
                                and urna.some_id = voto.some_id
                                and urna.city_code = voto.city_code
                                and urna.uf = voto.uf
                                and urna.zone_code = voto.zone_code
                                and urna.section_code = voto.section_code) as urna,
                            voto.event_timestamp
                        FROM events_df voto
                WHERE voto.event_system = 'VOTA'
                and voto.event_description = '{FIM_VOTO}'
                order by voto.uf, voto.city_code, voto.zone_code, voto.section_code, voto.some_id, voto.event_timestamp) x) Y ) Z
group by z.city_code
order by tempo_cidade desc
limit 10;
"""

query_tempo_medio_voto_uf=f"""
select z.uf,
    avg(z.tempo_voto) as tempo_voto_uf
from (select y.uf,
            y.city_code,
            y.zone_code,
            y.section_code,
            y.urna_num,
            y.urna,
            y.evento,
            (y.tempo_final - julian(y.data_inicio)) * 1440 as tempo_voto,
            y.data_inicio,
            y.event_timestamp as data_final
        from (select x.*,
                    (select max(vt.event_timestamp)
                        from events_df vt
                        where vt.event_system = x.event_system
                        and vt.event_description = '{ANCORA}'
                        and vt.uf = x.uf
                        and vt.city_code = x.city_code
                        and vt.zone_code = x.zone_code
                        and vt.section_code = x.section_code
                        and vt.some_id = x.urna_num
                        and julian(vt.event_timestamp) < x.tempo_final) as data_inicio
                from (SELECT voto.uf,
                            voto.city_code,
                            voto.zone_code,
                            voto.section_code,
                            voto.event_system,
                            voto.some_id as urna_num,
                            julian(voto.event_timestamp) as tempo_final,
                            voto.event_description as evento,
                            (SELECT GROUP_CONCAT(event_description) as urna_info
                                FROM events_df urna
                                WHERE urna.event_system = 'GAP'
                                and urna.some_id = voto.some_id
                                and urna.city_code = voto.city_code
                                and urna.uf = voto.uf
                                and urna.zone_code = voto.zone_code
                                and urna.section_code = voto.section_code) as urna,
                            voto.event_timestamp
                        FROM events_df voto
                WHERE voto.event_system = 'VOTA'
                and voto.event_description = '{FIM_VOTO}'
                order by voto.uf, voto.city_code, voto.zone_code, voto.section_code, voto.some_id, voto.event_timestamp) x) Y ) Z
group by z.uf
order by tempo_voto_uf desc 
limit 10;
"""

query_count_todos_registros="""
SELECT COUNT(*) as QuantidadeRegistros FROM events_df
"""

query_qtde_registros_eventos_sistema="""
 SELECT 
        event_system,
        COUNT(*) AS qtd_linhas
    FROM events_df
    GROUP BY event_system
"""

query_qtde_erro_alerta_info="""SELECT 
        event_type,
        COUNT(*) AS qtd_linhas
    FROM events_df
    WHERE event_system='VOTA' OR event_system='RED'
    GROUP BY event_type
"""
