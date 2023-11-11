'''
Provides functionality to get statistics much like that
of pd.Dataframe.info()
'''
import pandas as pd
import numpy as np


def _percentile_count(series: pd.Series, percentile_value: float = None) -> int:
    '''
    Returns the unique count of values for given series & percentile value
    '''
    if percentile_value is None:
        raise ValueError("percentile_value cannot be None")

    # retains the values where the values is in the percentile range and counts unique values
    return len(series[series < percentile_value].unique())


def _disjoint(a, b):
    '''
    Returns False if atleast both have atleast one value in common
    '''

    return set(a).isdisjoint(set(b))


def dataframe_info(
        df: pd.DataFrame,
        to_skip_cols: list = None,
        unique_count_cols: list = None,
        verbose_cols: list = None) -> pd.DataFrame:
    '''
    Returns the statistics for the given dataframe

    By default, "Columns", "Data Type", "Non-Null Count" columns are returned.
    However, with additional arguments if a list is passed to unique_count_cols
    is passed "Unique Count" column is also returned.

    For the columns passed as the verbose_cols list, these additional columns are also
    returned:

    "75th Percentile",
    "75th Percentile - Unique Count",
    "90th Percentile",
    "90th Percentile - Unique Count",
    "95th Percentile",
    "95th Percentile - Unique Count"
    '''

    # Basic checks to ensure arguments are passed correctly.
    # Checks if the any of the verbose/unique columns arg is passed
    # and that the values inside do not have overlapping values in
    # to_skip_cols argument as well
    if to_skip_cols is not None:

        if (verbose_cols is not None) and not _disjoint(to_skip_cols, verbose_cols):
            raise ValueError(
                "Column(s) in 'verbose_cols' are also in 'to_skip_cols'")

        if (unique_count_cols is not None) and not _disjoint(to_skip_cols, unique_count_cols):
            raise ValueError(
                "Column(s) in 'unique_count_cols' are also in 'to_skip_cols'")

        # creates a list of the columns values and removes the ones that need to be skipped
        columns = list(df.columns)

        for col in to_skip_cols:
            columns.remove(col)

    # summary will be a dictionary set up in such a way that it will finally be passed
    # to pd.Dataframe() as argument in order to return a dataframe as output.
    summary: dict[str, list] = {'Columns': columns}

    # returns the datatype of each column as interpreted by the pandas engine
    # str() here returns the result of __str__()
    summary['Data Type']: list = [
        str(df[col].dtype) for col in columns
    ]

    # returns the non-null count of values for each column
    summary['Non-Null Count'] = [
        df[col].dropna().count() for col in columns
    ]

    # If the unique_count_cols is not empty, then it goes through a for loop
    # for all the columns in columns-list.
    # Then, if columns is found in unique_columns list, it returns a count of
    # unique values else returns NaN.
    if unique_count_cols is not None:
        summary['Unique Count']: list = [
            len(df[col].unique()) if col in unique_count_cols else np.nan for col in columns]

        # for col in columns:

        #     if col in unique_count_cols:
        #         summary['Unique Count'].append(len(df[col].unique()))
        #     else:
        # summary['Unique Count'].append(np.nan)

    # if verbose columns list is not None, then returns the detailed statistics for these columns
    if verbose_cols is not None:

        # different detailed statistics that need to be computed for the mentioned columns
        statistics: list = ['Minimum', 'Maximum', 'Mean', 'Median', '75th Percentile',
                            '75th Percentile - Unique Count', '90th Percentile',
                            '90th Percentile - Unique Count', '95th Percentile',
                            '95th Percentile - Unique Count']

        # initializes a list for each of the item in statistics list as key in summary dict
        for item in statistics:
            summary[item]: list = []

        # goes through each of the columns in the list and then if column is in verbose' list
        # it computes the detailed statistics
        for i, col in enumerate(columns):

            if col in verbose_cols:

                # returns the minimum and maximum values
                summary['Minimum'].append(df[col].min())
                summary['Maximum'].append(df[col].max())

                # returns the average/mean and median values
                summary['Mean'].append(df[col].mean())
                summary['Median'].append(df[col].quantile())

                # returns the 75th percentile value & unique count of values in the
                # 75th percentile range
                summary['75th Percentile'].append(df[col].quantile(0.75))
                summary['75th Percentile - Unique Count'].append(
                    _percentile_count(
                        series=df[col],
                        percentile_value=summary['75th Percentile'][i]
                    )
                )

                # returns the 90th percentile value & unique count of values in the
                # 90th percentile range
                summary['90th Percentile'].append(df[col].quantile(0.90))
                summary['90th Percentile - Unique Count'].append(
                    _percentile_count(
                        series=df[col],
                        percentile_value=summary['90th Percentile'][i]
                    )
                )

                # returns the 95th percentile value & unique count of values in the
                # 95th percentile range
                summary['95th Percentile'].append(df[col].quantile(0.95))
                summary['95th Percentile - Unique Count'].append(
                    _percentile_count(
                        series=df[col],
                        percentile_value=summary['95th Percentile'][i]
                    )
                )

            # if the column is not in the verbose' list then it return a NaN
            else:

                for item in statistics:
                    summary[item].append(np.nan)

    # returns the summary dictionary as a dataframe
    return pd.DataFrame(summary)
