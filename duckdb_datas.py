import duckdb
import os
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 0)
pd.set_option('display.max_colwidth', None)

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
    F"{COLUMN_EVENT_DESCRIPTION} = 'Aguardando digitação do título'",
    F"{COLUMN_EVENT_DESCRIPTION} = 'Título digitado pelo mesário'",
    F"{COLUMN_EVENT_DESCRIPTION} = 'Eleitor foi habilitado'",
    F"{COLUMN_EVENT_DESCRIPTION} ILIKE 'Voto confirmado par%'",
    F"{COLUMN_EVENT_DESCRIPTION} = 'O voto do eleitor foi computado'",
    
    # BIOMETRIA
    F"{COLUMN_EVENT_DESCRIPTION} ILIKE '%Digital%' ",
    F"{COLUMN_EVENT_DESCRIPTION} ILIKE 'Dedo reconhecido%' ",
    F"{COLUMN_EVENT_DESCRIPTION} ILIKE 'Solicita digital%' ",
    F"{COLUMN_EVENT_DESCRIPTION} = 'Solicitação de dado pessoal do eleitor para habilitação manual' ",
]

ACCEPTED_DATES = [
    '2024-10-02', '2024-10-30', # Data constitucional da eleição
    '2024-10-03', '2024-10-31', # No caso da seção 'virar a noite' e acabar depois da meia noite, imagino que sejam casos RARÍSSIMOS
]

ALL_FILTERS = METADATA + EVENTS_DESCRIPTIONS + VOTES_DESCRIPTIONS

csv_path = './ALL_UFS.csv'

query = F"""
    SELECT 
        *
    FROM (
        SELECT
            event_timestamp,
            event_timestamp::date AS event_date,
            event_type,
            some_id,
            event_system,
            event_description,
            event_id,
                
            REPLACE(SPLIT_PART(filename, '/', 5), '_new.csv', '') AS filename,
            
            -- Metadata from filename
            SUBSTRING( SPLIT_PART(SPLIT_PART(filename, '/', 5), '-', 2),  1, 5 ) AS city_code,
            SUBSTRING( SPLIT_PART(SPLIT_PART(filename, '/', 5), '-', 2),  6, 4 ) AS zone_code,
            SUBSTRING( SPLIT_PART(SPLIT_PART(filename, '/', 5), '-', 2), 10, 4 ) AS section_code,
            REPLACE(SPLIT_PART(filename, '/', 4), '2_', '') AS uf
        FROM
            read_csv_auto('{csv_path}')
        WHERE 1=1
            AND ( {' OR '.join(ALL_FILTERS)} )
    ) _
    WHERE 1=1
    AND event_date IN ({', '.join([F"'{date}'" for date in ACCEPTED_DATES])})
"""

if not os.path.exists(csv_path):
    raise FileNotFoundError(f'O arquivo {csv_path} não foi encontrado.')

con = duckdb.connect()

result = con.execute(query).fetchdf()

# Mostra o resultado
print(result)
