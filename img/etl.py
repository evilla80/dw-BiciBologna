import pandas as pd
import json
from shapely.geometry import shape, Point
from datetime import datetime

print("--- INIZIO ETL ---")

# 1. CARICAMENTO DATI (EXTRACT)
print("1. Caricamento file...")
# Carichiamo il traffico (notare il separatore tab)
df_bici = pd.read_csv('colonnine_2020_2025.csv', sep='\t')
# Carichiamo il meteo
df_meteo = pd.read_csv('meteo_api.csv')
# Carichiamo i quartieri (JSON)
with open('quartieri_api.json', 'r') as f:
    quartieri_json = json.load(f)

# 2. TRASFORMAZIONI PRELIMINARI (CLEANING)
print("2. Pulizia e Normalizzazione date...")

# Convertiamo le date in datetime objects
# Bici ha la timezone, Meteo no. Rimuoviamo la timezone per uniformare
df_bici['data_dt'] = pd.to_datetime(df_bici['data']).dt.tz_localize(None)
df_meteo['time_dt'] = pd.to_datetime(df_meteo['time'])

# Arrotondiamo i minuti per essere sicuri che il join funzioni (es. 10:00:00)
df_bici['join_date'] = df_bici['data_dt'].dt.floor('h')
df_meteo['join_date'] = df_meteo['time_dt'].dt.floor('h')

# ---------------------------------------------------------
# 3. CREAZIONE DIM_COLONNINA (CON SPATIAL JOIN)
# ---------------------------------------------------------
print("3. Creazione DIM_COLONNINA (Spatial Join)...")

# Estraiamo le colonnine uniche
dim_colonnina = df_bici[['colonnina', 'geo_point_2d']].drop_duplicates().reset_index(drop=True)
dim_colonnina['id_colonnina'] = dim_colonnina.index + 1 # Chiave Surrogata

# Funzione per trovare il quartiere
def trova_quartiere(geo_point_str):
    try:
        # Il formato è "lat, lon" stringa
        lat, lon = map(float, geo_point_str.split(','))
        point = Point(lon, lat) # Shapely vuole (lon, lat)
        
        for feature in quartieri_json:
            polygon = shape(feature['geo_shape'])
            if polygon.contains(point):
                return feature['quartiere'] # O il nome del campo nel JSON
    except:
        return "Sconosciuto"
    return "Fuori Bologna"

# Applichiamo la funzione (Questo è il cuore dell'arricchimento ODS!)
dim_colonnina['quartiere'] = dim_colonnina['geo_point_2d'].apply(trova_quartiere)

# Puliamo la tabella finale
dim_colonnina[['lat', 'lon']] = dim_colonnina['geo_point_2d'].str.split(',', expand=True)
dim_colonnina = dim_colonnina[['id_colonnina', 'colonnina', 'quartiere', 'lat', 'lon']]
print(f"   -> Create {len(dim_colonnina)} colonnine con quartieri associati.")

# ---------------------------------------------------------
# 4. CREAZIONE DIM_ORA (LOGICA BUSINESS)
# ---------------------------------------------------------
print("4. Creazione DIM_ORA...")
hours = range(24)
dim_ora = pd.DataFrame({'ora_24': hours})
dim_ora['id_ora'] = dim_ora['ora_24'] # PK semplice

def get_fascia(h):
    if 6 <= h < 12: return 'Mattina'
    elif 12 <= h < 18: return 'Pomeriggio'
    elif 18 <= h < 24: return 'Sera'
    else: return 'Notte'

def is_picco(h):
    return (7 <= h <= 9) or (17 <= h <= 19)

dim_ora['fascia_oraria'] = dim_ora['ora_24'].apply(get_fascia)
dim_ora['is_orario_di_punta'] = dim_ora['ora_24'].apply(is_picco)

# ---------------------------------------------------------
# 5. CREAZIONE DIM_METEO
# ---------------------------------------------------------
print("5. Creazione DIM_METEO...")
# Qui assumiamo che il meteo sia unico per data/ora
dim_meteo = df_meteo.copy()
dim_meteo['id_meteo'] = dim_meteo.index + 1 # PK Surrogata
# Decodifica Weather Code (Esempio semplificato WMO)
wmo_codes = {0: 'Sereno', 1: 'Poco Nuvoloso', 2: 'Nuvoloso', 3: 'Coperto', 61: 'Pioggia', 63: 'Pioggia', 65: 'Pioggia Forte'}
dim_meteo['descrizione'] = dim_meteo['weather_code'].map(wmo_codes).fillna('Altro')
dim_meteo = dim_meteo[['id_meteo', 'join_date', 'temperature_2m', 'descrizione', 'weather_code']]

# ---------------------------------------------------------
# 6. CREAZIONE DIM_DATA
# ---------------------------------------------------------
print("6. Creazione DIM_DATA...")
date_uniche = df_bici['join_date'].dt.date.unique()
dim_data = pd.DataFrame({'data_date': date_uniche})
dim_data['id_data'] = dim_data['data_date'].apply(lambda x: int(x.strftime('%Y%m%d'))) # PK Intera Smart
dim_data['anno'] = pd.to_datetime(dim_data['data_date']).dt.year
dim_data['mese'] = pd.to_datetime(dim_data['data_date']).dt.month
dim_data['giorno_sett'] = pd.to_datetime(dim_data['data_date']).dt.day_name()
# Qui potresti aggiungere la logica "is_festivo" con una libreria tipo holidays

# ---------------------------------------------------------
# 7. CREAZIONE FACT TABLE (ASSEMBLAGGIO)
# ---------------------------------------------------------
print("7. Creazione FACT_TRANSITI...")

# Join colonna traffico -> Dim Colonnina
fact = df_bici.merge(dim_colonnina, left_on='colonnina', right_on='colonnina', how='left')

# Join colonna traffico -> Dim Meteo (su data ora)
fact = fact.merge(dim_meteo, left_on='join_date', right_on='join_date', how='left')

# Calcolo ID Data e ID Ora dai dati originali
fact['id_data'] = fact['join_date'].dt.strftime('%Y%m%d').astype(int)
fact['ora_24'] = fact['join_date'].dt.hour
fact = fact.merge(dim_ora[['ora_24', 'id_ora']], on='ora_24', how='left')

# Selezione colonne finali (Solo ID e Misure)
fact_table = fact[[
    'id_data', 
    'id_ora', 
    'id_colonnina', 
    'id_meteo', 
    'totale', 
    'direzione_centro', 
    'direzione_periferia'
]]

# Gestione valori nulli (es. meteo mancante)
fact_table = fact_table.fillna(-1) 

print(f"   -> Fact Table generata con {len(fact_table)} righe.")

# 8. SALVATAGGIO (LOAD)
dim_colonnina.to_csv('dw_dim_colonnina.csv', index=False)
dim_meteo.to_csv('dw_dim_meteo.csv', index=False)
dim_ora.to_csv('dw_dim_ora.csv', index=False)
dim_data.to_csv('dw_dim_data.csv', index=False)
fact_table.to_csv('dw_fact_transiti.csv', index=False)

print("--- ETL COMPLETATO. FILE CSV GENERATI ---")