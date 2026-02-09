import duckdb
import os

# IL PERCORSO CORRETTO: Il file .ddb nella cartella principale
db_path = "bike_sharing.ddb"
output_dir = "export_tableau"

# Creazione cartella output
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

print(f"Tentativo di connessione al database reale: {os.path.abspath(db_path)}")

if not os.path.exists(db_path):
    print(f"❌ ERRORE CRITICO: Il file '{db_path}' non esiste nella cartella corrente!")
    print("Assicurati di eseguire questo script dalla cartella 'dw-progetto'.")
else:
    try:
        # Connessione al database
        con = duckdb.connect(db_path, read_only=True)

        # Le tabelle del tuo Data Mart
        tables = [
            'dm_rilevazione',
            'dm_colonnina',
            'dm_quartiere',
            'dm_meteo',
            'dm_data',
            'dm_ora'
        ]

        print("--- Inizio Esportazione ---")

        for table in tables:
            try:
                # Dbt ha scritto nello schema 'main_datamart' (visto dai tuoi log precedenti)
                # Se fallisce, prova 'main'
                schema = 'main_datamart'

                query = f"COPY {schema}.{table} TO '{output_dir}/{table}.csv' (HEADER, DELIMITER ',')"
                con.execute(query)
                print(f"✅ Esportata: {table} (da {schema})")

            except duckdb.CatalogException:
                # Fallback: proviamo lo schema 'main' se non sono nel datamart
                try:
                    schema = 'main'
                    query = f"COPY {schema}.{table} TO '{output_dir}/{table}.csv' (HEADER, DELIMITER ',')"
                    con.execute(query)
                    print(f"✅ Esportata: {table} (da {schema})")
                except Exception as e:
                    print(f"❌ Errore su {table}: La tabella non è stata trovata nemmeno in 'main'.")

            except Exception as e:
                print(f"❌ Errore generico su {table}: {e}")

        con.close()
        print(f"\n🎉 Finito! I file CSV sono pronti nella cartella '{output_dir}'.")

    except Exception as e:
        print(f"❌ Errore di connessione: {e}")