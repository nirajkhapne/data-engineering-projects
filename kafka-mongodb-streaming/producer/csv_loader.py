import pandas as pd


def load_csv(path: str):
    df = pd.read_csv(path)

    object_cols = df.select_dtypes(include=['object']).columns
    df[object_cols] = df[object_cols].fillna('unknown')

    return df
