import duckdb
import pandas as pd
import os

# Configurazione per vedere bene le tabelle nel terminale
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.float_format', '{:.1f}'.format)

db_path = 'bike_sharing.ddb'
con = duckdb.connect(db_path)

con.execute("USE main_datamart")


print("1. VERIFICA JOIN SPAZIALE (Colonnine -> Quartieri)")
query_geo = """
SELECT COUNT(*) as totale_righe_effettive 
FROM dm_rilevazione; -- oppure ods_rilevazione se il dm non è ancora pronto
"""
print(con.execute(query_geo).df())

print("2. VERIFICA DATA PRIMA RILEVAZIONE (Data Installazione)")
query_start_date = """
SELECT 
    c.nome_colonnina,
    MIN(d.data) as prima_rilevazione,    -- Questa è la "data di installazione"
    MAX(d.data) as ultima_rilevazione,   -- Per vedere se è ancora attiva
    COUNT(*) as numero_righe_totali      -- Volume dati grezzo
FROM dm_rilevazione r
JOIN dm_data d ON r.data_idData = d.id_data
JOIN dm_colonnina c ON r.colonnina_idColonnina = c.id_colonnina
GROUP BY c.nome_colonnina
ORDER BY prima_rilevazione ASC;
"""
print(con.execute(query_start_date).df())

count = con.execute("SELECT COUNT(*) FROM dm_rilevazione").fetchone()[0]
teorico = 5 * 365 * 24 * 23

print(f"Numero dati")
print(f"Righe Reali (Fact Table): {count}")
print(f"Righe Teoriche (Max):     {teorico}")
print(f"Copertura Dati:           {round((count/teorico)*100, 2)}%")



con.close()