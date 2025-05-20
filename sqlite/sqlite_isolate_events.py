import sqlite3
import time

conn = sqlite3.connect('events.db')
cur = conn.cursor()

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
            DATE(SUBSTR(event_timestamp, 7, 4) || '-' || SUBSTR(event_timestamp, 4, 2) || '-' || SUBSTR(event_timestamp, 1, 2)) AS event_date,
            event_type,
            some_id,
            event_system,
            event_description,
            event_id,
            filename,
            REPLACE(filename, '_new.csv', '') AS cleaned_filename,
            SUBSTR(filename, 2, 5) AS city_code,
            SUBSTR(filename, 7, 2) AS uf,
            SUBSTR(filename, 7, 4) AS zone_code,
            SUBSTR(filename, 11, 4) AS section_code
        FROM events
        WHERE ({' OR '.join(ALL_FILTERS)})
    ) AS CONSULTA
    WHERE event_date IN ({accepted_dates_sql});"""

start = time.perf_counter()
cur.execute("DROP TABLE IF EXISTS votos_por_uf;")  # Remove se já existir
cur.execute(f"""
    CREATE TABLE votos_por_uf AS
    {query}
""")
conn.commit()
end = time.perf_counter()

print(f"Tabela criada em {end - start:.2f} segundos.")
conn.close()
