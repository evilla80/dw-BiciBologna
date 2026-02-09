import requests
import pandas as pd

def download_bici_2020_2025():
    url = "https://opendata.comune.bologna.it/api/explore/v2.1/catalog/datasets/colonnine-conta-bici/exports/csv"

    where_clause = 'data >= "2021-01-01" and data <= "2025-12-31"'

    params = {
        'where': where_clause,
        'lang': 'it',
        'timezone': 'Europe/Rome',
        'delimiter': '\t'
    }

    print(f"Esecuzione query API: {where_clause}")
    response = requests.get(url, params=params)

    if response.status_code == 200:
        with open('colonnine_2021_2025.csv', 'wb') as f:
            f.write(response.content)
        print("File salvato con successo: colonnine_2021_2025.csv")
    else:
        print(f"Errore nella chiamata API: {response.status_code}")


def download_meteo_data():
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 44.49,
        "longitude": 11.34,
        "start_date": "2021-01-01",  # Nuovo inizio
        "end_date": "2025-12-31",  # Fine anno scorso
        "hourly": "temperature_2m,apparent_temperature,weather_code",
        "timezone": "Europe/Berlin"
    }

    print("Richiesta dati meteo via API...")
    response = requests.get(url, params=params)
    data = response.json()

    # Trasformiamo la risposta JSON in un DataFrame tabellare
    df_meteo = pd.DataFrame(data['hourly'])
    df_meteo.to_csv('meteo_api.csv', index=False)
    print("Download completato: meteo_api.csv")

def download_quartieri_data():
    url = "https://opendata.comune.bologna.it/api/explore/v2.1/catalog/datasets/quartieri-di-bologna/exports/json"

    print("Scaricamento confini quartieri...")
    response = requests.get(url)

    with open('quartieri_api.json', 'wb') as f:
        f.write(response.content)
    print("Download completato: quartieri_api.json")


download_bici_2020_2025()
download_meteo_data()
download_quartieri_data()