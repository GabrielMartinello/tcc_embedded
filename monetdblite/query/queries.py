ANCORA = 'aguardando digitação do identificador do eleitor'
FIM_VOTO = 'o voto do eleitor foi computado'

query_tempo_medio_voto_por_zona=f"""
select z.zone_code,
    avg(z.tempo_voto) as tempo_medio_zona
from (select y.uf,
            y.city_code,
            y.zone_code,
            y.section_code,
            y.urna_num,
            y.urna,
            y.evento,
            (y.tempo_final - julianday(y.data_inicio)) * 1440 as tempo_voto,
            y.data_inicio,
            y.event_timestamp as data_final
        from (select x.*,
                    (select max(vt.event_timestamp)
                        from eventos vt
                        where vt.event_system = x.event_system
                        and vt.event_description = '{ANCORA}'
                        and vt.uf = x.uf
                        and vt.city_code = x.city_code
                        and vt.zone_code = x.zone_code
                        and vt.section_code = x.section_code
                        and vt.some_id = x.urna_num
                        and julianday(vt.event_timestamp) < x.tempo_final) as data_inicio
                from (SELECT voto.uf,
                            voto.city_code,
                            voto.zone_code,
                            voto.section_code,
                            voto.event_system,
                            voto.some_id as urna_num,
                            julianday(voto.event_timestamp) as tempo_final,
                            voto.event_description as evento,
                            (SELECT GROUP_CONCAT(event_description) as urna_info
                                FROM eventos urna
                                WHERE urna.event_system = 'GAP'
                                and urna.some_id = voto.some_id
                                and urna.city_code = voto.city_code
                                and urna.uf = voto.uf
                                and urna.zone_code = voto.zone_code
                                and urna.section_code = voto.section_code) as urna,
                            voto.event_timestamp
                        FROM eventos voto
                WHERE voto.event_system = 'VOTA'
                and voto.event_description = '{FIM_VOTO}'
                order by voto.uf, voto.city_code, voto.zone_code, voto.section_code, voto.some_id, voto.event_timestamp) x) Y ) Z
group by z.zone_code
order by tempo_medio_zona desc
limit 10
"""

query_top_10_pessoas_que_mais_demoraram=f"""select y.uf,
       y.city_code,
       y.zone_code,
       y.urna,
       (extract(day from (y.event_timestamp - y.data_inicio)) * 24 * 60 +
         extract(hour from (y.event_timestamp - y.data_inicio)) * 60 +
         extract(minute from (y.event_timestamp - y.data_inicio))) AS tempo_voto,
       y.data_inicio,
       y.event_timestamp as data_final
from (select x.*,
             (select max(vt.event_timestamp)
              from eventos vt
              where vt.event_system = x.event_system
                and vt.event_description = '{ANCORA}'
                and vt.uf = x.uf
                and vt.city_code = x.city_code
                and vt.zone_code = x.zone_code
                and vt.section_code = x.section_code
                and vt.some_id = x.urna_num
                and vt.event_timestamp < x.tempo_final) as data_inicio
      from (SELECT voto.uf,
                   voto.city_code,
                   voto.zone_code,
                   voto.section_code,
                   voto.event_system,
                   voto.some_id as urna_num,
                   voto.event_timestamp as tempo_final,
                   voto.event_description as evento,
                   (SELECT GROUP_CONCAT(event_description) as urna_info
                    FROM eventos urna
                    WHERE urna.event_system = 'GAP'
                      and urna.some_id = voto.some_id
                      and urna.city_code = voto.city_code
                      and urna.uf = voto.uf
                      and urna.zone_code = voto.zone_code
                      and urna.section_code = voto.section_code) as urna,
                   voto.event_timestamp
            FROM eventos voto
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
            (y.tempo_final - julianday(y.data_inicio)) * 1440 as tempo_voto,
            y.data_inicio,
            y.event_timestamp as data_final
        from (select x.*,
                    (select max(vt.event_timestamp)
                        from eventos vt
                        where vt.event_system = x.event_system
                        and vt.event_description = '{ANCORA}'
                        and vt.uf = x.uf
                        and vt.city_code = x.city_code
                        and vt.zone_code = x.zone_code
                        and vt.section_code = x.section_code
                        and vt.some_id = x.urna_num
                        and julianday(vt.event_timestamp) < x.tempo_final) as data_inicio
                from (SELECT voto.uf,
                            voto.city_code,
                            voto.zone_code,
                            voto.section_code,
                            voto.event_system,
                            voto.some_id as urna_num,
                            julianday(voto.event_timestamp) as tempo_final,
                            voto.event_description as evento,
                            (SELECT GROUP_CONCAT(event_description) as urna_info
                                FROM eventos urna
                                WHERE urna.event_system = 'GAP'
                                and urna.some_id = voto.some_id
                                and urna.city_code = voto.city_code
                                and urna.uf = voto.uf
                                and urna.zone_code = voto.zone_code
                                and urna.section_code = voto.section_code) as urna,
                            voto.event_timestamp
                        FROM eventos voto
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
            (y.tempo_final - julianday(y.data_inicio)) * 1440 as tempo_voto,
            y.data_inicio,
            y.event_timestamp as data_final
        from (select x.*,
                    (select max(vt.event_timestamp)
                        from eventos vt
                        where vt.event_system = x.event_system
                        and vt.event_description = '{ANCORA}'
                        and vt.uf = x.uf
                        and vt.city_code = x.city_code
                        and vt.zone_code = x.zone_code
                        and vt.section_code = x.section_code
                        and vt.some_id = x.urna_num
                        and julianday(vt.event_timestamp) < x.tempo_final) as data_inicio
                from (SELECT voto.uf,
                            voto.city_code,
                            voto.zone_code,
                            voto.section_code,
                            voto.event_system,
                            voto.some_id as urna_num,
                            julianday(voto.event_timestamp) as tempo_final,
                            voto.event_description as evento,
                            (SELECT GROUP_CONCAT(event_description) as urna_info
                                FROM eventos urna
                                WHERE urna.event_system = 'GAP'
                                and urna.some_id = voto.some_id
                                and urna.city_code = voto.city_code
                                and urna.uf = voto.uf
                                and urna.zone_code = voto.zone_code
                                and urna.section_code = voto.section_code) as urna,
                            voto.event_timestamp
                        FROM eventos voto
                WHERE voto.event_system = 'VOTA'
                and voto.event_description = '{FIM_VOTO}'
                order by voto.uf, voto.city_code, voto.zone_code, voto.section_code, voto.some_id, voto.event_timestamp) x) Y ) Z
group by z.uf
order by tempo_voto_uf desc 
limit 10;
"""

query_count_todos_registros="""
SELECT COUNT(*) as QuantidadeRegistros FROM eventos
"""

query_qtde_registros_eventos_sistema="""
 SELECT 
        event_system,
        COUNT(*) AS qtd_linhas
    FROM eventos
    GROUP BY event_system
"""

query_qtde_erro_alerta_info="""SELECT 
        event_type,
        COUNT(*) AS qtd_linhas
    FROM eventos
    WHERE event_system='VOTA' OR event_system='RED'
    GROUP BY event_type
"""
