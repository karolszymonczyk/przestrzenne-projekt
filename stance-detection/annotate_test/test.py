from pprint import pprint

import numpy as np
import pandas as pd
from sklearn.metrics import cohen_kappa_score
from itertools import combinations
from krippendorff import alpha


def load_data(path: str) -> pd.DataFrame:
    return pd.read_csv(
        path,
        usecols=["id", "MW", "JP", "KS"],
        delimiter="\t",
        dtype=str,
    )


def calculate_cohen(df: pd.DataFrame, annotators: list):
    possible_pairs = list(combinations(annotators, 2))
    response_dict = {}
    for col_a, col_b in possible_pairs:
        sub_df = df[[col_a, col_b]].copy()
        sub_df.dropna(inplace=True)
        response_dict[(col_a, col_b)] = cohen_kappa_score(
            sub_df[col_a].tolist(), sub_df[col_b].tolist()
        )

    return response_dict


metrics = {}
test_files = ["data.tsv"]
annotators = ["MW", "JP", "KS"]


for path in test_files:
    df = load_data(path)
    arr = df[annotators].copy().to_numpy()
    metrics[path] = {
        "cohen kappa": calculate_cohen(df, annotators=annotators),
        "krippendorff alpha": alpha(np.transpose(arr), level_of_measurement="nominal"),
    }


pprint(metrics)
