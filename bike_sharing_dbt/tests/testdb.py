import duckdb
import os

db_path = '../../bike_sharing.ddb'
if not os.path.exists(db_path):
    print("ERRORE: Il file .ddb non esiste in questa cartella!")
else:
    con = duckdb.connect(db_path)
    # Mostra tutte le tabelle presenti
    tabelle = con.execute("SHOW TABLES").fetchall()
    print(f"Tabelle trovate: {tabelle}")
    con.close()


# Assicurati che il percorso sia giusto
db_path = '../../bike_sharing.ddb'
con = duckdb.connect(db_path)

print("--- COLONNE TABELLA raw_colonnine ---")
print(con.execute("DESCRIBE raw_colonnine").df()[['column_name', 'column_type']])

print("\n--- COLONNE TABELLA raw_quartieri ---")
print(con.execute("DESCRIBE raw_quartieri").df()[['column_name', 'column_type']])

print("\n--- COLONNE TABELLA raw_meteo ---")
print(con.execute("DESCRIBE raw_meteo").df()[['column_name', 'column_type']])

con.close()