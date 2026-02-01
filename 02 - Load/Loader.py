import duckdb
import os

current_dir = os.path.dirname(os.path.abspath(__file__))

# Costruisce il percorso risalendo alla radice del progetto
bici_path = os.path.abspath(os.path.join(
    current_dir, '..', '01_extraction', 'colonnine_2020_2025.csv'))
meteo_path = os.path.abspath(os.path.join(
    current_dir, '..', '01_extraction', 'meteo_api.csv'))
quartieri_path = os.path.abspath(os.path.join(
    current_dir, '..', '01_extraction', 'quartieri_api.json'))

db_path = os.path.join('..', 'bike_sharing.ddb')

# Connessione al database (nella root del progetto)
con = duckdb.connect(db_path)

print("Inizio caricamento dati in DuckDB...")

# 1. Caricamento Bici (CSV con separatore tabulazione \t)
con.execute(f"""
    CREATE OR REPLACE TABLE raw_bici AS 
    SELECT * FROM read_csv_auto('{bici_path}', delim='\t')
""")
print(f"--- Tabella raw_bici creata da {bici_path}")

# 2. Caricamento Meteo (CSV standard)
con.execute(f"""
    CREATE OR REPLACE TABLE raw_meteo AS 
    SELECT * FROM read_csv_auto('{meteo_path}')
""")
print(f"--- Tabella raw_meteo creata da {meteo_path}")

# 3. Caricamento Quartieri (JSON)
con.execute(f"""
    CREATE OR REPLACE TABLE raw_quartieri AS 
    SELECT 
        * FROM read_json_auto('{quartieri_path}')
""")

print(f"--- Tabella raw_quartieri creata da {quartieri_path}")

print("\nLoad completata con successo!")
con.close()