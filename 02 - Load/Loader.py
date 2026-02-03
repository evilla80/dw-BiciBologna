import duckdb
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)

# Percorso dei dati (CSV/JSON)
data_dir = os.path.join(project_root, '01_extraction')
# Percorso del Database (lo salviamo nella root del progetto)
db_path = os.path.join(project_root, 'bike_sharing.ddb')

# Verifica che i file esistano prima di provare
files_to_check = ['colonnine_2020_2025.csv', 'meteo_api.csv', 'quartieri_api.json']
for f in files_to_check:
    full_path = os.path.join(data_dir, f)
    if not os.path.exists(full_path):
        print(f"Non trovo il file {f}")
        exit()

con = duckdb.connect(db_path)
# Installazione e caricamento estensione spaziale
con.execute("INSTALL spatial; LOAD spatial;")

csv_bici = os.path.join(data_dir, 'colonnine_2020_2025.csv')
con.execute(f"CREATE OR REPLACE TABLE raw_colonnine AS SELECT * FROM read_csv_auto('{csv_bici}', delim='\\t')")

csv_meteo = os.path.join(data_dir, 'meteo_api.csv')
con.execute(f"CREATE OR REPLACE TABLE raw_meteo AS SELECT * FROM read_csv_auto('{csv_meteo}')")

print("Caricamento raw_quartieri...")
json_quartieri = os.path.join(data_dir, 'quartieri_api.json')
con.execute(f"CREATE OR REPLACE TABLE raw_quartieri AS SELECT * FROM read_json_auto('{json_quartieri}')")

count_bici = con.execute("SELECT COUNT(*) FROM raw_colonnine").fetchone()[0]
count_meteo = con.execute("SELECT COUNT(*) FROM raw_meteo").fetchone()[0]

print(f"\n--- SUCCESSO! ---")
print(f"Righe Bici caricate: {count_bici}")
print(f"Righe Meteo caricate: {count_meteo}")
print("Il file bike_sharing.ddb è pronto.")



con.close()