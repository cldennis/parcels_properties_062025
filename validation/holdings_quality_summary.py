#!/usr/bin/env python3
"""
holdings_quality_summary.py

Compute “valid parcel” rates (clear + likely), summary statistics,
and category breakdowns for one or more holdings‐sample CSVs.

Usage:
    python holdings_quality_summary.py holdings_region1.csv holdings_region2.csv ...
"""

import sys
import os
import pandas as pd

def process_holdings(df):
    """
    Given a DataFrame with columns:
      - clear_fit_pct
      - likely_pct
      - unsure/inconsistent_pct  (optional)
    computes:
      - valid_pct = clear_fit_pct + likely_pct
      - summary stats on valid_pct
      - % of holdings ≥ 95% valid
      - category assignment: Review (<80), Good (80–95), Excellent (>95)
    Returns a dict of results.
    """
    # 1. Compute combined valid percentage
    df['valid_pct'] = df['clear_fit_pct'] + df['likely_pct']

    # 2. Summary statistics
    stats = df['valid_pct'].agg(['mean', 'std', 'min', 'max']).to_dict()

    # 3. Holdings ≥ 95% valid
    pct_high95 = (df['valid_pct'] >= 95).mean() * 100

    # 4. Category bins
    bins = [0, 80, 95, 100]
    labels = ['Review', 'Good', 'Excellent']
    df['category'] = pd.cut(
        df['valid_pct'],
        bins=bins,
        labels=labels,
        right=True,
        include_lowest=True
    )
    cat_counts = (df['category']
                  .value_counts(normalize=True)
                  .reindex(labels, fill_value=0)
                  * 100
                 ).to_dict()

    return {
        'mean_valid_pct':  stats['mean'],
        'std_valid_pct':   stats['std'],
        'min_valid_pct':   stats['min'],
        'max_valid_pct':   stats['max'],
        'pct_high95':      pct_high95,
        'pct_review':      cat_counts['Review'],
        'pct_good':        cat_counts['Good'],
        'pct_excellent':   cat_counts['Excellent'],
    }

def main():
    if len(sys.argv) < 2:
        print("Usage: python holdings_quality_summary.py <file1.csv> [file2.csv ...]")
        sys.exit(1)

    summary_rows = []
    for path in sys.argv[1:]:
        region_name = os.path.splitext(os.path.basename(path))[0]
        print(f"\n=== Processing {region_name} ===")
        df = pd.read_csv(path)

        # Ensure required columns exist
        for col in ('clear_fit_pct','likely_pct'):
            if col not in df.columns:
                raise ValueError(f"Missing required column '{col}' in {path}")

        results = process_holdings(df)

        # Print results for this region
        print(f"Mean valid_pct: {results['mean_valid_pct']:.1f}% "
              f"(SD {results['std_valid_pct']:.1f}%, "
              f"range {results['min_valid_pct']:.1f}–{results['max_valid_pct']:.1f})")
        print(f"% holdings ≥ 95% valid parcels: {results['pct_high95']:.1f}%")
        print("Category breakdown:")
        print(f"  Review (<80%):     {results['pct_review']:.1f}%")
        print(f"  Good (80–95%):      {results['pct_good']:.1f}%")
        print(f"  Excellent (>95%):   {results['pct_excellent']:.1f}%")

        # Save for combined summary
        row = {'region': region_name}
        row.update(results)
        summary_rows.append(row)

    # Combine region summaries
    summary_df = pd.DataFrame(summary_rows)
    print("\n=== Combined Summary ===")
    print(summary_df.to_string(index=False))

    # Optionally, write combined summary to CSV
    out_csv = "holdings_regions_summary.csv"
    summary_df.to_csv(out_csv, index=False)
    print(f"\nCombined summary written to {out_csv}")

if __name__ == "__main__":
    main()
