import duckdb
import os

db_path = 'bike_sharing.ddb'
if not os.path.exists(db_path):
    print("ERRORE: Il file .ddb non esiste in questa cartella!")
else:
    con = duckdb.connect(db_path)
    # Mostra tutte le tabelle presenti
    tabelle = con.execute("SHOW TABLES").fetchall()
    print(f"Tabelle trovate: {tabelle}")
    con.close()