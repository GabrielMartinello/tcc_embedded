import sqlite3
import time

conn = sqlite3.connect('events.db')
cur = conn.cursor()

cur_select = conn.cursor() 
cur_insert = conn.cursor()

print("Criando tabela 'votos_por_uf' no SQLite...")

ACCEPTED_DATES = ['2024-10-27', '2024-11-29', '2024-10-28', '2024-11-30']
COLUMN_EVENT_DESCRIPTION = 'event_description'

METADATA = [
    F"{COLUMN_EVENT_DESCRIPTION} LIKE 'Zona Eleitoral%'",
    F"{COLUMN_EVENT_DESCRIPTION} LIKE 'Seção Eleitoral%'",
    F"{COLUMN_EVENT_DESCRIPTION} LIKE 'Município%'",
    F"{COLUMN_EVENT_DESCRIPTION} LIKE 'Local de Votação%'",
    F"{COLUMN_EVENT_DESCRIPTION} LIKE 'Turno da UE%'",
    F"{COLUMN_EVENT_DESCRIPTION} LIKE 'Identificação do Modelo de Urna%'"
]

EVENTS_DESCRIPTIONS = [
    F"{COLUMN_EVENT_DESCRIPTION} LIKE 'Urna pronta para receber vot%'",
]

VOTES_DESCRIPTIONS = [
    # VOTOS
    F"{COLUMN_EVENT_DESCRIPTION} = 'Aguardando digitação do identificador do eleitor'",
    F"{COLUMN_EVENT_DESCRIPTION} = 'Identificador do eleitor digitado pelo mesário'",
    F"{COLUMN_EVENT_DESCRIPTION} = 'Eleitor foi habilitado'",
    F"{COLUMN_EVENT_DESCRIPTION} LIKE 'Voto confirmado par%'",
    F"{COLUMN_EVENT_DESCRIPTION} = 'O voto do eleitor foi computado'",
    
    # BIOMETRIA
    F"{COLUMN_EVENT_DESCRIPTION} LIKE '%Digital%' ",
    F"{COLUMN_EVENT_DESCRIPTION} LIKE 'Tipo de habilitação do eleitor [biométrica]%' ",
    F"{COLUMN_EVENT_DESCRIPTION} LIKE 'Solicita digital%' ",
    F"{COLUMN_EVENT_DESCRIPTION} = 'Solicitação de dado pessoal do eleitor para habilitação manual' ",
]

ALL_FILTERS = METADATA + EVENTS_DESCRIPTIONS + VOTES_DESCRIPTIONS

accepted_dates_sql = ', '.join(f"'{d}'" for d in ACCEPTED_DATES)

query=f"""SELECT * FROM (
        SELECT 
            event_timestamp,
            DATE(event_timestamp) AS event_date,
            event_type,
            some_id,
            event_system,
            event_description,
            event_id,
            filename,
            SUBSTR(filename, 2, 5) AS city_code,
            SUBSTR(filename, 7, 2) AS uf,
            SUBSTR(filename, 7, 4) AS zone_code,
            SUBSTR(filename, 11, 4) AS section_code,
            '' as ident_id
        FROM events
        WHERE ({' OR '.join(ALL_FILTERS)})
    ) AS CONSULTA
    WHERE event_date IN ({accepted_dates_sql});"""

start = time.perf_counter()
print(query)

cur_insert.execute("DROP TABLE IF EXISTS votos_por_uf;")
cur_insert.execute('''
    CREATE TABLE votos_por_uf (
        event_timestamp DATETIME,
        event_date DATE,
        event_type TEXT,
        some_id TEXT,
        event_system TEXT,
        event_description TEXT,
        event_id TEXT,
        filename TEXT,
        city_code TEXT,
        uf TEXT,
        zone_code TEXT,
        section_code TEXT,
        ident_id INTEGER
    )
''')

controleVoto = 0
ids_computados = set()
batch = []
batch_size = 1000

#Otimizado pra não explodir o PC
for row in cur_select.execute(query):
    event_timestamp = row[0]
    event_date = row[1]
    event_type = row[2]
    some_id = row[3]
    event_system = row[4]
    event_description = row[5]
    event_id = row[6]
    filename = row[7]
    city_code = row[8]
    uf = row[9]
    zone_code = row[10]
    section_code = row[11]
    ident_id = row[12]

    if (some_id not in ids_computados and event_system == 'GAP' and 'Identificação do Modelo' in event_description):
        controleVoto += 1
        ids_computados.add(some_id)

    if event_system == 'VOTA' and event_description == 'Urna pronta para receber votos':
        controleVoto += 1

    ident_id = controleVoto

    if event_system == 'VOTA' and event_description == 'O voto do eleitor foi computado':
        controleVoto += 1

    batch.append((
        event_timestamp,
        event_date,
        event_type,
        some_id,
        event_system,
        event_description,
        event_id,
        filename,
        city_code,
        uf,
        zone_code,
        section_code,
        ident_id
    ))

    # Otimizacao pra nao matar o PC, pq foi o que quase aconteceu com o meu
    if len(batch) >= batch_size:
        cur_insert.executemany("INSERT INTO votos_por_uf VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", batch)
        conn.commit()
        batch.clear()

if batch:
    cur_insert.executemany("INSERT INTO votos_por_uf VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", batch)
    conn.commit()

# Criar índices
createIndexQuery = """
CREATE INDEX event_index ON votos_por_uf (event_description);
CREATE INDEX event_time_stamp_index ON votos_por_uf (event_timestamp);
CREATE INDEX some_id_index ON votos_por_uf (some_id);
CREATE INDEX event_system_index ON votos_por_uf (event_system);
CREATE INDEX ident_id ON votos_por_uf (ident_id);
CREATE INDEX key_votos_por_uf ON votos_por_uf (ident_id, event_system, some_id, city_code, uf, zone_code, section_code);
"""
cur_insert.executescript(createIndexQuery)
conn.commit()

end = time.perf_counter()

print(f"Tabela criada em {end - start:.2f} segundos.")
conn.close()