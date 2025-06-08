import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder


# -- Konfiguracja projektu --
TABLE = "liquid-tractor-462013-t5.football_dataset.all_matches"
from google.oauth2 import service_account

from google.oauth2 import service_account
from google.cloud import bigquery

from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file("/etc/secrets/credentials.json")
client = bigquery.Client(credentials=credentials, project=credentials.project_id)



st.set_page_config(page_title="⚽ Analiza meczów piłkarskich", layout="wide")
st.title("⚽ Analiza meczów piłkarskich w Europie (20 sezonów)")

# -- Pobieranie dostępnych opcji --
@st.cache_data
def get_options():
    query = f"SELECT DISTINCT league, season FROM `{TABLE}`"
    df = client.query(query).to_dataframe()
    return sorted(df["league"].unique()), sorted(df["season"].unique())

leagues, seasons = get_options()

# -- Wybór ligi i sezonu --
col1, col2 = st.columns(2)
with col1:
    selected_league = st.selectbox("🏆 Wybierz ligę:", leagues)
with col2:
    selected_season = st.selectbox("📅 Wybierz sezon:", seasons)

# -- Pobieranie danych meczu --
@st.cache_data
def load_filtered(league, season):
    query = f"""
    SELECT Date, HomeTeam, AwayTeam, FTHG, FTAG, FTR
    FROM `{TABLE}`
    WHERE league = '{league}' AND season = '{season}'
    ORDER BY Date
    """
    return client.query(query).to_dataframe()

df = load_filtered(selected_league, selected_season)

# -- Wyświetlanie danych i statystyki --
st.markdown(f"### 📄 Mecze: {selected_league} – sezon {selected_season}")
st.dataframe(df)

if not df.empty:
    total_matches = len(df)
    total_goals = (df["FTHG"] + df["FTAG"]).sum()
    avg_goals = round(total_goals / total_matches, 2)
    home_wins = len(df[df["FTR"] == "H"])
    away_wins = len(df[df["FTR"] == "A"])
    draws = len(df[df["FTR"] == "D"])

    st.markdown("### 📊 Statystyki sezonu")
    col1, col2, col3 = st.columns(3)
    col1.metric("🔢 Liczba meczów", total_matches)
    col2.metric("⚽ Średnia goli na mecz", avg_goals)
    col3.metric("➗ Remisy", draws)

    col4, col5 = st.columns(2)
    col4.metric("🏠 Wygrane gospodarzy", home_wins)
    col5.metric("🛫 Wygrane gości", away_wins)

    st.markdown("### 🧮 Najczęstsze wyniki")
    df["Wynik"] = df["FTHG"].astype(str) + ":" + df["FTAG"].astype(str)
    wynik_counts = df["Wynik"].value_counts().sort_values(ascending=False)

    fig, ax = plt.subplots()
    wynik_counts.head(10).plot(kind='bar', ax=ax)
    ax.set_xlabel("Wynik meczu")
    ax.set_ylabel("Liczba spotkań")
    st.pyplot(fig)

    st.markdown("### 📥 Eksport danych")
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Pobierz jako CSV",
        data=csv,
        file_name=f"{selected_league}_{selected_season}_matches.csv",
        mime="text/csv",
    )
else:
    st.warning("⚠️ Brak danych dla wybranej kombinacji ligi i sezonu.")

# ------------------------------
# 🔮 PRZEWIDYWANIE WYNIKU MECZU
# ------------------------------

st.markdown("---")
st.header("🔮 Przewidywanie wyniku meczu (tylko w obrębie jednej ligi)")

@st.cache_data
def load_model_data():
    query = f"""
    SELECT season, league, HomeTeam, AwayTeam, FTR
    FROM `{TABLE}`
    WHERE FTHG IS NOT NULL AND FTAG IS NOT NULL
    AND season < '2324'
    """
    return client.query(query).to_dataframe()

model_df = load_model_data()
selected_prediction_league = st.selectbox("🏆 Wybierz ligę do predykcji:", sorted(model_df["league"].unique()))
df_league = model_df[model_df["league"] == selected_prediction_league]

le_home = LabelEncoder()
le_away = LabelEncoder()
df_league["HomeTeam_enc"] = le_home.fit_transform(df_league["HomeTeam"])
df_league["AwayTeam_enc"] = le_away.fit_transform(df_league["AwayTeam"])

X = df_league[["HomeTeam_enc", "AwayTeam_enc"]]
y = df_league["FTR"]
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X, y)

teams = sorted(set(df_league["HomeTeam"]) | set(df_league["AwayTeam"]))

col1, col2 = st.columns(2)
with col1:
    team_home = st.selectbox("🏠 Gospodarz", teams, key="home")
with col2:
    team_away = st.selectbox("🛫 Gość", teams, key="away")

if st.button("🔍 Przewiduj wynik"):
    if team_home == team_away:
        st.error("⚠️ Wybierz dwie różne drużyny.")
    else:
        if team_home in le_home.classes_ and team_away in le_away.classes_:
            home_enc = le_home.transform([team_home])[0]
            away_enc = le_away.transform([team_away])[0]
            prediction = model.predict([[home_enc, away_enc]])[0]
            proba = model.predict_proba([[home_enc, away_enc]])[0]
            outcome_map = {"H": "🏠 Gospodarz wygra", "D": "➗ Remis", "A": "🛫 Gość wygra"}
            st.success(f"🔮 Przewidywany wynik: {outcome_map[prediction]}")
            st.markdown(f"""
                **Prawdopodobieństwa**:
                - 🏠 Gospodarz: {round(proba[model.classes_ == 'H'][0] * 100, 1)}%
                - ➗ Remis: {round(proba[model.classes_ == 'D'][0] * 100, 1)}%
                - 🛫 Gość: {round(proba[model.classes_ == 'A'][0] * 100, 1)}%
            """)
        else:
            st.warning("⚠️ Jedna z drużyn nie ma wystarczającej historii w danych.")
