import pandas as pd
from pathlib import Path

region = "midwest"

def summarize_holdings(csv_path, summary_csv_path):
    """
    Reads a CSV with a 'holdid' column plus N category columns
    (each row has exactly one 'y' in one of those columns),
    and writes out a summary CSV giving, for each holdid,
    the percentage of rows with 'y' in each category column.
    """
    # 1) load everything as strings
    df = pd.read_csv(csv_path, dtype=str)

    # 2) identify the category columns (everything except 'holdid')
    cat_cols = [c for c in df.columns if c != 'holdid']

    # 3) convert 'y' to 1, everything else to 0
    df[cat_cols] = df[cat_cols].eq('y').astype(int)

    # 4) group by holdid and take the mean of those 0/1 columns → fraction of y’s
    pct = (df
           .groupby('holdid')[cat_cols]
           .mean()  # e.g. 0.23 for 23%
           * 100  # convert to percentage
           )

    # 5) rename columns if you like, e.g. add “_pct”
    pct = pct.rename(columns={c: f"{c}_pct" for c in cat_cols})

    # 6) reset index so holdid is a column again
    summary = pct.reset_index()

    # 7) write it out
    Path(summary_csv_path).parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(summary_csv_path, index=False)

    print(f"Wrote {len(summary)} holdings to {summary_csv_path!r}")
    return summary


if __name__ == "__main__":
    in_csv = f"/home/christina/Desktop/property-matching/regrid_2025/validation/holdings100/sampled_{region}_100parcel_holdings.csv"
    out_csv = f"/home/christina/Desktop/property-matching/regrid_2025/validation/holdings100/sampled_{region}_100parcel_holdings_summary.csv"
    summarize_holdings(in_csv, out_csv)

# ---- full summary
# def count_y_and_rows(csv_path):
#     """
#     Reads a CSV and returns a Series with:
#       - count of 'y' in each column
#       - a 'row_count' entry for total rows
#     """
#     df = pd.read_csv(csv_path, dtype=str)
#     y_counts = df.eq('y').sum()
#     y_counts['row_count'] = len(df)
#     return y_counts
#
#
# def summarize_folder(folder_path, summary_csv_path):
#     """
#     Globs folder_path/*.csv, counts 'y' per column & total rows in each file,
#     and writes a summary table to summary_csv_path.
#     """
#     folder = Path(folder_path)
#     records = []
#
#     for csv_file in folder.glob("*.csv"):
#         stats = count_y_and_rows(csv_file)
#         rec = stats.to_dict()
#         rec['file'] = csv_file.name
#         records.append(rec)
#
#     # build DataFrame: one row per file
#     summary_df = pd.DataFrame(records)
#
#     # reorder columns: file, row_count, then the rest
#     cols = ['file', 'row_count'] + [c for c in summary_df.columns
#                                     if c not in ('file', 'row_count')]
#     summary_df = summary_df[cols]
#
#     summary_df.to_csv(summary_csv_path, index=False)
#     print(f"Wrote summary for {len(records)} files to {summary_csv_path!r}")
#
#
# if __name__ == "__main__":
#     folder = "/home/christina/Desktop/property-matching/regrid_2025/validation/holdings100/"
#     out_csv = "/home/christina/Desktop/property-matching/regrid_2025/validation/holdings100/summary.csv"
#     summarize_folder(folder, out_csv)
