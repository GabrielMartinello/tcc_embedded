import sqlite3
import pandas as pd
import time

conn = sqlite3.connect('events.db')

start = time.perf_counter()
query="""
    select y.uf,
       y.city_code,
       y.zone_code,
       y.section_code,
       y.urna_num,
       y.urna,
       y.evento,
       (y.tempo_final - julianday(y.data_inicio )) * 1440 as tempo_voto,
       y.data_inicio,
       y.event_timestamp as data_final
from (select x.*,
             (select max(vt.event_timestamp)
              from votos_por_uf vt
              where vt.event_system = x.event_system
                and vt.event_description = 'Aguardando digitação do identificador do eleitor'
                and vt.uf = x.uf
                and vt.city_code = x.city_code
                and vt.zone_code = x.zone_code
                and vt.section_code = x.section_code
                and vt.some_id = x.urna_num
                and julianday(vt.event_timestamp) < x.tempo_final
              order by vt.event_timestamp desc) as data_inicio
      from (SELECT voto.uf,
                   voto.city_code,
                   voto.zone_code,
                   voto.section_code,
                   voto.event_system,
                   voto.some_id                                  as urna_num,
                   julianday(voto.event_timestamp)               as tempo_final,
                   voto.event_description                        as evento,
                   (SELECT GROUP_CONCAT(event_description) as urna_info
                    FROM votos_por_uf urna
                    WHERE urna.event_system = 'GAP'
                      and urna.some_id = voto.some_id
                      and urna.city_code = voto.city_code
                      and urna.uf = voto.uf
                      and urna.zone_code = voto.zone_code
                      and urna.section_code = voto.section_code) as urna,
                   voto.event_timestamp
            FROM votos_por_uf voto
            WHERE voto.event_system = 'VOTA'
              and voto.event_description = 'O voto do eleitor foi computado'
            order by voto.uf, voto.city_code, voto.zone_code, voto.section_code, voto.some_id, voto.event_timestamp) x) Y
            order by tempo_voto desc
			limit 10;
"""

conn.execute(query)
end = time.perf_counter()
print(f"A consulta demorou {end - start:.2f} segundos.")
conn.close()