import monetdblite
import time
import os

DB_PATH = './events_monet'

monetdblite.init(DB_PATH)
con = monetdblite.connect()

ACCEPTED_DATES = ['2024-10-27', '2024-11-29', '2024-10-28', '2024-11-30']

filters = [
    "event_description ILIKE 'Zona Eleitoral%'",
    "event_description ILIKE 'Seção Eleitoral%'",
    "event_description ILIKE 'Município%'",
    "event_description ILIKE 'Local de Votação%'",
    "event_description ILIKE 'Turno da UE%'",
    "event_description ILIKE 'Identificação do Modelo de Urna%'",
    "event_description ILIKE 'Urna pronta para receber vot%'",
    "event_description = 'Aguardando digitação do identificador do eleitor'",
    "event_description = 'Identificador do eleitor digitado pelo mesário'",
    "event_description = 'Eleitor foi habilitado'",
    "event_description ILIKE 'Voto confirmado par%'",
    "event_description = 'O voto do eleitor foi computado'",
    "event_description ILIKE '%Digital%'",
    "event_description ILIKE 'Tipo de habilitação do eleitor [biométrica]%'",
    "event_description ILIKE 'Solicita digital%'",
    "event_description = 'Solicitação de dado pessoal do eleitor para habilitação manual'"
]

COLUMN_EVENT_DESCRIPTION = 'event_description'

METADATA = [
    F"{COLUMN_EVENT_DESCRIPTION} ILIKE 'Zona Eleitoral%'",
    F"{COLUMN_EVENT_DESCRIPTION} ILIKE 'Seção Eleitoral%'",
    F"{COLUMN_EVENT_DESCRIPTION} ILIKE 'Município%'",
    F"{COLUMN_EVENT_DESCRIPTION} ILIKE 'Local de Votação%'",
    F"{COLUMN_EVENT_DESCRIPTION} ILIKE 'Turno da UE%'",
    F"{COLUMN_EVENT_DESCRIPTION} ILIKE 'Identificação do Modelo de Urna%'"
]

EVENTS_DESCRIPTIONS = [
    F"{COLUMN_EVENT_DESCRIPTION} ILIKE 'Urna pronta para receber vot%'",
]

VOTES_DESCRIPTIONS = [
    # VOTOS
    F"{COLUMN_EVENT_DESCRIPTION} = 'Aguardando digitação do identificador do eleitor'",
    F"{COLUMN_EVENT_DESCRIPTION} = 'Identificador do eleitor digitado pelo mesário'",
    F"{COLUMN_EVENT_DESCRIPTION} = 'Eleitor foi habilitado'",
    F"{COLUMN_EVENT_DESCRIPTION} ILIKE 'Voto confirmado par%'",
    F"{COLUMN_EVENT_DESCRIPTION} = 'O voto do eleitor foi computado'",
    
    # BIOMETRIA
    F"{COLUMN_EVENT_DESCRIPTION} ILIKE '%Digital%' ",
    F"{COLUMN_EVENT_DESCRIPTION} ILIKE 'Tipo de habilitação do eleitor [biométrica]%' ",
    F"{COLUMN_EVENT_DESCRIPTION} ILIKE 'Solicita digital%' ",
    F"{COLUMN_EVENT_DESCRIPTION} = 'Solicitação de dado pessoal do eleitor para habilitação manual' ",
]

ALL_FILTERS = METADATA + EVENTS_DESCRIPTIONS + VOTES_DESCRIPTIONS

accepted_dates_sql = ', '.join(f"'{d}'" for d in ACCEPTED_DATES)

query = f"""
    SELECT * FROM (
        SELECT 
            event_timestamp,
            CAST(
                SUBSTRING(event_timestamp, 7, 4) || '-' || SUBSTRING(event_timestamp, 4, 2) || '-' || SUBSTRING(event_timestamp, 1, 2) 
                AS DATE
            ) AS event_date,
            event_type,
            some_id,
            event_system,
            event_description,
            event_id,
            -- Extrações da UF e códigos da seção/município
            REPLACE(filename, '_new.csv', '') AS filename,
            SUBSTRING(filename, 2, 5) AS city_code,
            SUBSTRING(filename, 7, 2) AS uf,
            SUBSTRING(filename, 7, 4) AS zone_code,
            SUBSTRING(filename, 11, 4) AS section_code
        FROM events
        WHERE ({' OR '.join(ALL_FILTERS)})
    ) AS CONSULTA
    WHERE CONSULTA.event_date IN ({accepted_dates_sql})
"""

print("Criando nova tabela com os dados filtrados...")
start = time.perf_counter()
create_table_query = f"""
    CREATE TABLE votos_por_uf AS
    {query}
"""

monetdblite.sql(create_table_query, client=con)
print("Tabela 'votos_por_uf' criada com sucesso.")
end = time.perf_counter()
print(f"Eventos isolados em {end - start:.2f}s")

del con
