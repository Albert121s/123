from google.cloud import bigquery

project_id = "liquid-tractor-462013-t5"
dataset = "football_dataset"
view_name = "all_matches"
leagues = ['E0', 'D1', 'I1', 'SP1', 'F1']
seasons = [f"{str(y).zfill(2)}{str(y+1)[-2:]}" for y in range(3, 24)]

# Kolumny wspólne we wszystkich sezonach i ligach
COMMON_COLUMNS = ["Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"]

def generate_union_sql():
    parts = []
    for league in leagues:
        for season in seasons:
            table = f"`{project_id}.{dataset}.data_{league}_{season}`"
            cols = ", ".join(COMMON_COLUMNS)
            parts.append(
                f"SELECT {cols}, '{league}' AS league, '{season}' AS season FROM {table}"
            )
    return "\nUNION ALL\n".join(parts)

def create_view():
    client = bigquery.Client(project=project_id)
    sql = f"""
    CREATE OR REPLACE VIEW `{project_id}.{dataset}.{view_name}` AS
    {generate_union_sql()}
    """
    job = client.query(sql)
    job.result()
    print(f"✅ Widok `{view_name}` utworzony.")

if __name__ == "__main__":
    create_view()
