import pandas as pd
import requests
from io import StringIO
import os

# Lista sezonów i lig
seasons = [f"{str(y).zfill(2)}{str(y+1)[-2:]}" for y in range(3, 24)]

leagues = {
    'E0': 'Premier League',
    'D1': 'Bundesliga',
    'I1': 'Serie A',
    'SP1': 'La Liga',
    'F1': 'Ligue 1'
}

os.makedirs("data", exist_ok=True)

for season in seasons:
    for code, name in leagues.items():
        url = f"https://www.football-data.co.uk/mmz4281/{season}/{code}.csv"
        try:
            r = requests.get(url)
            r.raise_for_status()
            df = pd.read_csv(StringIO(r.text))
            file_path = f"data/{code}_{season}.csv"
            df.to_csv(file_path, index=False)
            print(f"Pobrano: {file_path}")
        except Exception as e:
            print(f"❌ Błąd: {url} – {e}")
