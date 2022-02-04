import demoji
import pandas as pd
import argparse
from tqdm.auto import tqdm
tqdm.pandas()


def parse(df: pd.DataFrame, text_col: str, new_col: str) -> pd.DataFrame:
    df[new_col] = df.progress_apply(lambda row: demoji.replace_with_desc(row[text_col]), axis=1)

    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse emojis into text, save it in a new column and export.')
    parser.add_argument('--output', required=True, type=str)
    parser.add_argument('--text-col', required=True, type=str)
    parser.add_argument('--new-col', required=True, type=str)
    parser.add_argument('--input', type=str, required=True)
    parser.add_argument('--filetype', type=str, choices=['jsonl', 'tsv'], default='tsv')


    args = parser.parse_args()

    if args.filetype == 'tsv':
        df = pd.read_csv(args.input, sep='\t', converters={'id': str, 'target': str})
    else:
        df = pd.read_json(args.input, lines=True, dtype=False)

    new_df = parse(df, args.text_col, args.new_col)
    new_df.to_csv(args.output, sep='\t', index=False)
    if args.filetype == 'tsv':
        new_df.to_csv(args.output, sep='\t', index=False, encoding='utf-8')
    else:
        new_df.to_json(args.output, lines=True, orient="records")

