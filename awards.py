import pandas as pd
from datetime import datetime

# Carrega o DataFrame de premiações a partir do Excel
def load_award_dates(path):
    df_awards = pd.read_excel(path)
    df_awards.columns = df_awards.columns.str.strip()  # remove espaços
    df_awards['Oscar'] = pd.to_datetime(df_awards['Oscar'], dayfirst=True)
    df_awards['Globo de Ouro'] = pd.to_datetime(df_awards['Globo de Ouro'], dayfirst=True)
    df_awards['Festival de Cannes'] = pd.to_datetime(df_awards['Festival de Cannes'], dayfirst=True)
    return df_awards

# Função para calcular dias até o Oscar do ano seguinte
def add_days_to_next_oscar(filmes_df, awards_df):
    def calc(row):
        release_year = row['release_date'].year + 1
        ref = awards_df[awards_df['Ano'] == release_year]
        return (ref['Oscar'].values[0] - row['release_date']).days if not ref.empty else pd.NA
    filmes_df['days_to_next_oscar'] = filmes_df.apply(calc, axis=1)
    return filmes_df

# Função para calcular dias até o Globo de Ouro do ano seguinte
def add_days_to_next_golden_globe(filmes_df, awards_df):
    def calc(row):
        release_year = row['release_date'].year + 1
        ref = awards_df[awards_df['Ano'] == release_year]
        return (ref['Globo de Ouro'].values[0] - row['release_date']).days if not ref.empty else pd.NA
    filmes_df['days_to_next_golden_globe'] = filmes_df.apply(calc, axis=1)
    return filmes_df

# Função para calcular dias desde o último Festival de Cannes
def add_days_from_last_cannes(filmes_df, awards_df):
    def calc(row):
        release_year = row['release_date'].year
        ref = awards_df[awards_df['Ano'] <= release_year].sort_values('Ano', ascending=False)
        return (row['release_date'] - ref['Festival de Cannes'].values[0]).days if not ref.empty else pd.NA
    filmes_df['days_from_last_cannes'] = filmes_df.apply(calc, axis=1)
    return filmes_df
