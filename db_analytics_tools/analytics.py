# coding : utf-8

"""
    DB Analytics Tools Data Analysis
"""

import pandas as pd
import numpy as np


def pick_sample(dataframe, sample_size, bins=5, ignore=[], verbose=0):
    """
    Sample a dataframe using stratified sampling based on categorical features 
    and discretized numerical features.

    :param dataframe: pandas.DataFrame : The source dataframe to sample from.
    :param sample_size: float : The fraction of the population to return (0 to 1).
    :param bins: int, optional : Number of quantiles to use for numerical discretization, default 5.
    :param ignore: list, optional : Columns to exclude from the stratification logic, default [].
    :param verbose: int, optional : If 1, displays comparison statistics between population and sample.
    :return: pd.DataFrame : The representative sample.
    """
    df = dataframe.copy()
    n_reps = 100

    # 1. Identify column types excluding ignored columns
    cols_numeric = [col for col in df.select_dtypes(include=[np.number]).columns.tolist() if col not in ignore]
    cols_categorical = [col for col in df.select_dtypes(exclude=[np.number]).columns.tolist() if col not in ignore]

    if verbose == 1:
        print(f"{'='*n_reps}")
        print(f"SAMPLING PROCESS START")
        print(f"{'='*n_reps}")
        print(f"Quantitative Columns (Stratified): {cols_numeric}")
        print(f"Qualitative Columns (Stratified): {cols_categorical}")
        print(f"Ignored Columns: {ignore}\n")

    # 2. Discretize numerical variables for stratification
    # We use quintiles (or custom bins) to ensure numerical representation
    stratify_cols = list(cols_categorical)

    for col in cols_numeric:
        strat_col_name = f"{col}_bins"
        try:
            # Create categorical bins based on quantiles
            df[strat_col_name] = pd.qcut(df[col], q=bins, labels=False, duplicates='drop')
            stratify_cols.append(strat_col_name)
        except ValueError:
            # Fallback if there are not enough unique values for qcut
            df[strat_col_name] = df[col]
            stratify_cols.append(strat_col_name)

    # 3. Perform stratified sampling
    # Grouping by all stratification columns ensures proportional representation
    sampled_df = df.groupby(stratify_cols, group_keys=False).apply(
        lambda x: x.sample(frac=sample_size, random_state=42) if not x.empty else x,
        # include_groups=False
    )

    # 4. Cleanup: remove temporary discretization columns
    temp_cols = [c for c in sampled_df.columns if c.endswith('_bins')]
    sampled_df = sampled_df.drop(columns=temp_cols)
    df_clean = df.drop(columns=temp_cols)
    
    # 5. Distribution Comparison
    if verbose == 1:
        # A. Qualitative Variables Comparison
        print(f"{'-'*n_reps}")
        print(f"CATEGORICAL VARIABLES COMPARISON (%)")
        print(f"{'-'*n_reps}")
        for col in cols_categorical:
            pop_dist = df_clean[col].value_counts(normalize=True) * 100
            sam_dist = sampled_df[col].value_counts(normalize=True) * 100
            
            comp_cat = pd.DataFrame({
                'Population (%)': pop_dist,
                'Sample (%)': sam_dist
            }).fillna(0)
            
            print(f"\nFeature: {col}")
            print(comp_cat.to_string(float_format="{:.2f}".format))
            print("-" * 30)

        # B. Quantitative Variables Comparison
        print(f"\n{'-'*n_reps}")
        print(f"NUMERICAL VARIABLES COMPARISON (Mean & Std)")
        print(f"{'-'*n_reps}")

        num_stats = pd.DataFrame(index=cols_numeric)
        num_stats['Pop_Mean'] = df_clean[cols_numeric].mean()
        num_stats['Sample_Mean'] = sampled_df[cols_numeric].mean()
        num_stats['Pop_Std'] = df_clean[cols_numeric].std()
        num_stats['Sample_Std'] = sampled_df[cols_numeric].std()

        print(num_stats.to_string(float_format="{:.4f}".format))
        print(f"\nPopulation size: {len(df_clean)}")
        print(f"Sample size:     {len(sampled_df)}")
        print(f"{'='*n_reps}\n")

    return sampled_df
