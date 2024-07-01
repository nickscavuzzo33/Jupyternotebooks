import requests
import pandas as pd
from sqlalchemy import create_engine

# Extract
def fetch_github_data(username, repo):
    url = f"https://api.github.com/repos/{username}/{repo}"
    response = requests.get(url)
    return response.json()

# Transform
def process_data(data):
    processed = {
        'name': data['name'],
        'stars': data['stargazers_count'],
        'forks': data['forks_count'],
        'open_issues': data['open_issues_count']
    }
    return pd.DataFrame([processed])

# Load
def load_to_database(df, table_name):
    engine = create_engine('sqlite:///github_portfolio.db')
    df.to_sql(table_name, engine, if_exists='append', index=False)

# Main ETL process
def github_etl(username, repo):
    raw_data = fetch_github_data(username, repo)
    processed_data = process_data(raw_data)
    load_to_database(processed_data, 'repositories')

# Run the ETL
github_etl('your_username', 'your_repo')