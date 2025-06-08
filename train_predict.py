import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report

from google.cloud import bigquery

PROJECT_ID = "liquid-tractor-462013-t5"
TABLE = "football_dataset.all_matches"

# 1. Pobierz dane z BigQuery
client = bigquery.Client(project=PROJECT_ID)
query = f"""
SELECT
  season,
  league,
  HomeTeam,
  AwayTeam,
  FTHG,
  FTAG,
  FTR
FROM `{PROJECT_ID}.{TABLE}`
WHERE FTHG IS NOT NULL AND FTAG IS NOT NULL
"""
df = client.query(query).to_dataframe()

# 2. Przygotowanie danych
df = df[df["season"] < "2223"]  # treningowe dane (do 2022/23)

# Kodowanie drużyn
le_home = LabelEncoder()
le_away = LabelEncoder()
df["HomeTeam_enc"] = le_home.fit_transform(df["HomeTeam"])
df["AwayTeam_enc"] = le_away.fit_transform(df["AwayTeam"])

# Klasa do przewidzenia
y = df["FTR"]
X = df[["HomeTeam_enc", "AwayTeam_enc"]]

# 3. Podział i trenowanie modelu
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

clf = RandomForestClassifier(n_estimators=100, random_state=42)
clf.fit(X_train, y_train)

# 4. Ewaluacja
y_pred = clf.predict(X_test)
print(classification_report(y_test, y_pred))

# 5. Przykład predykcji
home = "Manchester United"
away = "Chelsea"

home_enc = le_home.transform([home])[0] if home in le_home.classes_ else -1
away_enc = le_away.transform([away])[0] if away in le_away.classes_ else -1

if home_enc >= 0 and away_enc >= 0:
    pred = clf.predict([[home_enc, away_enc]])[0]
    print(f"🔮 Przewidywany wynik: {home} vs {away} → {pred}")
else:
    print("⚠️ Jedna z drużyn nie występuje w danych treningowych.")
