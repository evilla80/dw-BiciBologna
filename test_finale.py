import duckdb
import pandas as pd
import os

# Configurazione per vedere bene le tabelle nel terminale
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.float_format', '{:.1f}'.format)

db_path = 'bike_sharing.ddb'
con = duckdb.connect(db_path)

def esegui_query(titolo, sql):
    print(f"\n--- {titolo} ---")
    try:
        df = con.execute(sql).df()
        if df.empty:
            print("⚠️ Nessun dato trovato.")
        else:
            print(df)
    except Exception as e:
        print(f"❌ Errore: {e}")


sql_mesi = """
SELECT 
    d.mese,
    SUM(f.totale) as passaggi_totali
FROM dm_rilevazione f
JOIN dm_data d ON f.data_idData = d.idData
GROUP BY d.mese
ORDER BY passaggi_totali DESC;
"""
esegui_query("1. In quali mesi si usa di più la bici?", sql_mesi)

sql_ore = """
SELECT 
    o.ora,
    o.fascia_oraria,
    SUM(f.totale) as passaggi_totali
FROM dm_rilevazione f
JOIN dm_ora o ON f.ora_idOra = o.idOra
GROUP BY o.ora, o.fascia_oraria
ORDER BY passaggi_totali DESC
LIMIT 5;
"""
esegui_query("2. Quali sono le ore di punta assolute?", sql_ore)


sql_weekend = """
SELECT 
    CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Feriale' END as tipo_giorno,
    ROUND(AVG(f.totale), 2) as media_passaggi
FROM dm_rilevazione f
JOIN dm_data d ON f.data_idData = d.idData
GROUP BY d.is_weekend;
"""
esegui_query("3. Si usa la bici più per lavoro o per svago?", sql_weekend)


sql_quartieri = """
SELECT 
    q.nome_quartiere,
    SUM(f.totale) as passaggi_totali,
    COUNT(DISTINCT c.idColonnina) as num_colonnine_attive
FROM dm_rilevazione f
JOIN dm_colonnina c ON f.colonnina_idColonnina = c.idColonnina
JOIN dm_quartiere q ON c.quartiere_idQuartiere = q.idQuartiere
GROUP BY q.nome_quartiere
ORDER BY passaggi_totali DESC;
"""
esegui_query("4. Dove c'è più traffico ciclabile?", sql_quartieri)


sql_top_colonnine = """
SELECT 
    c.nome_colonnina,
    q.nome_quartiere,
    SUM(f.totale) as passaggi_totali
FROM dm_rilevazione f
JOIN dm_colonnina c ON f.colonnina_idColonnina = c.idColonnina
JOIN dm_quartiere q ON c.quartiere_idQuartiere = q.idQuartiere
GROUP BY c.nome_colonnina, q.nome_quartiere
ORDER BY passaggi_totali DESC
LIMIT 5;
"""
esegui_query("5. Quali sono le colonnine più trafficate", sql_top_colonnine)


sql_meteo = """
SELECT 
    m.fascia_temperatura,
    ROUND(AVG(f.totale), 2) as media_passaggi
FROM dm_rilevazione f
JOIN dm_meteo m ON f.meteo_idMeteo = m.idMeteo
WHERE m.fascia_temperatura IS NOT NULL
GROUP BY m.fascia_temperatura
ORDER BY media_passaggi DESC;
"""
esegui_query("6. Il freddo scoraggia i ciclisti?", sql_meteo)

sql_pioggia_gen = """
SELECT 
    CASE 
        WHEN m.descrizione ILIKE '%nev%' THEN 'Neve'
        WHEN m.descrizione ILIKE '%pioggia%' OR m.descrizione ILIKE '%temporale%' THEN 'Pioggia'
        ELSE 'Tempo stabile' 
    END as condizione_meteo,
    ROUND(AVG(f.totale), 0) as media_passaggi,
    SUM(f.totale) as totale_passaggi
    
FROM dm_rilevazione f
JOIN dm_meteo m ON f.meteo_idMeteo = m.idMeteo
GROUP BY condizione_meteo
ORDER BY media_passaggi DESC;
"""
esegui_query("7. Come le precipitazioni influenzano i flussi?", sql_pioggia_gen)

sql_pendolari = """
SELECT 
    o.ora,
    SUM(f.direzioneCentro) as verso_centro,
    SUM(f.direzionePeriferia) as verso_periferia,
    CASE 
        WHEN SUM(f.direzioneCentro) > SUM(f.direzionePeriferia) THEN 'Flusso -> Centro'
        ELSE 'Flusso -> Periferia'
    END as tendenza_dominante
FROM dm_rilevazione f
JOIN dm_ora o ON f.ora_idOra = o.idOra
WHERE o.ora IN (8, 18) -- Guardiamo solo picco mattina e picco sera
GROUP BY o.ora
ORDER BY o.ora;
"""
esegui_query("8. La mattina si va verso il centro e la sera si torna in periferia?", sql_pendolari)


sql_densita = """
SELECT 
    q.nome_quartiere,
    SUM(f.totale) as passaggi_totali,
    -- Visualizziamo la superficie in km quadrati (moltiplicando per 8800)
    CAST(MAX(q.superficie) * 8800 AS DECIMAL(10,2)) as superficie_kmq,
    
    -- Calcoliamo la densità: Passaggi diviso km quadrati
    CAST(SUM(f.totale) / (MAX(q.superficie) * 8800) AS INTEGER) as passaggi_per_kmq
FROM dm_rilevazione f
JOIN dm_colonnina c ON f.colonnina_idColonnina = c.idColonnina
JOIN dm_quartiere q ON c.quartiere_idQuartiere = q.idQuartiere
GROUP BY q.nome_quartiere
ORDER BY passaggi_per_kmq DESC;
"""
esegui_query("9. Qual è il quartiere più 'intenso' in rapporto alla sua grandezza?", sql_densita)

con.close()