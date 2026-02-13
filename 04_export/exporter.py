import duckdb
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(script_dir, "export_tableau")

project_root = os.path.dirname(script_dir)
db_path = os.path.join(project_root, "bike_sharing.ddb")

if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    print(f"Cartella creata: {output_dir}")

print(f"Tentativo di connessione al database reale: {os.path.abspath(db_path)}")


try:
    con = duckdb.connect(db_path, read_only=True)
    tables = ['dm_rilevazione', 'dm_colonnina', 'dm_quartiere', 'dm_meteo', 'dm_data', 'dm_ora']

    print("Inizio Esportazione")
    for table in tables:
        schema = 'main_datamart'
        query = f"COPY {schema}.{table} TO '{output_dir}/{table}.csv' (HEADER, DELIMITER ',')"
        con.execute(query)

    con.close()
    print(f"I file CSV sono pronti.")

except Exception as e:
        print(f"Errore nell'esportazione: {e}")