import datetime as dt
import pandas as pd
import numpy as np
import pandas as pd
from typing import List, Optional, Tuple
from utils.utils import *
from mapping_transfer_check.basic_functions import *
from mapping_transfer_check.duplicates_functions import *

'''
The general idea: we will progressively populate the df_rapproche dataframe with already matched lines
    Params:
        + df_payment: DataFrame containing client payment data (check or transfer table)
        + df_BO: BO orders/contracts not yet processed
        + list_cols_Clientname: List of columns regarding the payer in the transfer/check table
        + date_colname: Name of the Date column in the payment table
        + amount_colname: Name of the Amount column in the payment table
        + payment_id: Name of the system ID column (unique ID) in the payment table
        + amount_threshold: Acceptance threshold on the amount for matching
        + motif: The method of matching (unique payment / npaiement_1ord, etc.) to trigger the function and to flag in the final result
        + nb_days_period: Maximum number of days accepted between the creation date of the orders and the payment date
        + min_score: Minimum accepted percentage for the similarity score
'''

def mapping_unique_payment(
    df_rapproche: pd.DataFrame,
    df_payment: pd.DataFrame,
    df_BO: pd.DataFrame,
    list_cols_Clientname: List[str],
    **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    This function matches payments with BO orders based on a basic rule. It attempts to match 
    payment records with BO orders using one or more customer name columns. Matched payments and 
    orders are then removed from the original DataFrames to avoid duplicate matches.

    Parameters:
    - df_rapproche (pd.DataFrame): DataFrame containing matched records.
    - df_payment (pd.DataFrame): DataFrame containing payment records.
    - df_BO (pd.DataFrame): DataFrame containing BO order records.
    - list_cols_Clientname (List[str]): List of customer name columns to use for matching in the payment DataFrame.
    - kwargs: Additional optional parameters for matching:
        - date_colname (Optional[str]): Name of the date column.
        - amount_colname (Optional[str]): Name of the amount column.
        - payment_id (Optional[str]): Name of the payment ID column.
        - motif (Optional[str]): Motif to add to the matched records.
        - amount_threshold (int): Threshold amount for matching (default is 5).
        - min_score (int): Minimum score for matching (default is 90).

    Returns:
    - Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: Updated DataFrames for matched records, remaining payments, and remaining BO orders.
    """
    # Extract keyword arguments with defaults
    date_colname = kwargs.get('date_colname')
    amount_colname = kwargs.get('amount_colname')
    payment_id = kwargs.get('payment_id')
    motif = kwargs.get('motif')
    amount_threshold = kwargs.get('amount_threshold', 5)
    min_score = kwargs.get('min_score', 90)
    
    for clientname_col in list_cols_Clientname:
        df_match = rapprocher_paiement_bo_basic(
            df_payment, df_BO, clientname_col, date_colname, amount_colname, payment_id,
            amount_threshold, min_score
        )
        df_match['motif'] = motif
        df_rapproche = pd.concat([df_rapproche, df_match], ignore_index=True)
        
        # Filter out matched records
        matched_payment_ids = set(df_match[payment_id])
        matched_order_ids = set(df_match['order_id'])
        
        df_payment = df_payment[~df_payment[payment_id].isin(matched_payment_ids)]
        df_BO = df_BO[~df_BO['order_id'].isin(matched_order_ids)]
    return df_rapproche,df_payment,df_BO

def mapping_npaiement_1ord(
    df_rapproche: pd.DataFrame,
    df_payment: pd.DataFrame,
    df_BO: pd.DataFrame,
    list_cols_Clientname: List[str],
    **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    This function matches multiple payments to a single BO order. 
    It aggregates payment data over a date interval and uses the mapping_unique_payment function 
    because aggregated payments are treated as a single payment.

    Parameters:
    - df_rapproche (pd.DataFrame): DataFrame containing matched records.
    - df_payment (pd.DataFrame): DataFrame containing client payment data (check or transfer table).
    - df_BO (pd.DataFrame): BO orders/contracts not yet processed.
    - list_cols_Clientname (List[str]): List of columns regarding the payer in the transfer/check table.
    - kwargs: Additional optional parameters for matching:
        - date_colname (Optional[str]): Name of the Date column in the payment table.
        - amount_colname (Optional[str]): Name of the Amount column in the payment table.
        - payment_id (Optional[str]): Name of the system ID column (unique ID) in the payment table.
        - motif (Optional[str]): The method of matching to flag in the final result.
        - nb_days_period (int): Maximum number of days accepted between the creation date of the orders and the payment date.
        - amount_threshold (float): Acceptance threshold on the amount for matching.
        - min_score (int): Minimum accepted percentage for the similarity score.
        - is_lightcheck (bool): Flag to indicate if a light check should be performed.

    Returns:
    - Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: Updated DataFrames for matched records, remaining payments, and remaining BO orders.
    """
    # Extract keyword arguments with defaults
    date_colname = kwargs.get('date_colname')
    amount_colname = kwargs.get('amount_colname')
    payment_id = kwargs.get('payment_id')
    motif = kwargs.get('motif')
    nb_days_period = kwargs.get('nb_days_period', 2)
    amount_threshold = kwargs.get('amount_threshold', 5)
    min_score = kwargs.get('min_score', 90)
    is_lightcheck = kwargs.get('is_lightcheck', False)

    # Calculate the number of days to aggregate
    nbdays_agreges = int(nb_days_period / 2)
    for clientname_col in list_cols_Clientname:
        date_colname_2 = 'Max_' + date_colname
        amount_colname_2 = amount_colname + '_total'
        step = 2
        for nb_days in np.arange(4, nbdays_agreges + step, step=step):
            # Aggregate payment data by date interval
            df_payment_agg = aggregate_by_date(df_payment, payment_id, clientname_col, date_colname, amount_colname, nb_days)
            if len(df_payment_agg) > 0:
                # Match with the aggregated payment
                if is_lightcheck:
                    df_match = create_light_check(df_payment_agg, df_BO, clientname_col, date_colname_2, amount_colname_2, payment_id, amount_threshold)
                else:
                    df_match = rapprocher_paiement_bo_basic(df_payment_agg, df_BO, clientname_col, date_colname_2, amount_colname_2, payment_id,
                                                            amount_threshold, min_score)
                if len(df_match) > 0:
                    list_col = list(df_BO.columns)
                    list_col.append(payment_id)
                    df_match = df_match[list_col]

                    # Revert to unit payments
                    df_match[payment_id] = df_match[payment_id].str.split('|')
                    df_match = df_match.explode(payment_id, ignore_index=True)
                    df_match = df_match.merge(df_payment, on=payment_id)
                    df_match['motif'] = motif
                    df_rapproche = pd.concat([df_rapproche, df_match])
                    df_payment = df_payment[~df_payment[payment_id].isin(df_rapproche[payment_id])]
                    df_BO = df_BO[~df_BO.order_id.isin(df_rapproche.order_id)]
    return df_rapproche, df_payment, df_BO

def mapping_1paiement_nord(
    df_rapproche: pd.DataFrame,
    df_payment: pd.DataFrame,
    df_BO: pd.DataFrame,
    list_cols_Clientname: List[str],
    **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    This function matches multiple BO orders to a single payment.
    It aggregates BO data over a date interval and uses the mapping_unique_payment function 
    because aggregated orders are treated as a single order.

    Parameters:
    - df_rapproche (pd.DataFrame): DataFrame containing matched records.
    - df_payment (pd.DataFrame): DataFrame containing client payment data (check or transfer table).
    - df_BO (pd.DataFrame): BO orders/contracts not yet processed.
    - list_cols_Clientname (List[str]): List of columns regarding the payer in the transfer/check table.
    - kwargs: Additional optional parameters for matching:
        - date_colname (Optional[str]): Name of the Date column in the payment table.
        - amount_colname (Optional[str]): Name of the Amount column in the payment table.
        - payment_id (Optional[str]): Name of the system ID column (unique ID) in the payment table.
        - motif (Optional[str]): The method of matching to flag in the final result.
        - nb_days_period (int): Maximum number of days accepted between the creation date of the orders and the payment date.
        - amount_threshold (float): Acceptance threshold on the amount for matching.
        - min_score (int): Minimum accepted percentage for the similarity score.

    Returns:
    - Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: Updated DataFrames for matched records, remaining payments, and remaining BO orders.
    """
    # Extract keyword arguments with defaults
    date_colname = kwargs.get('date_colname')
    amount_colname = kwargs.get('amount_colname')
    payment_id = kwargs.get('payment_id')
    motif = kwargs.get('motif')
    nb_days_period = kwargs.get('nb_days_period', 2)
    amount_threshold = kwargs.get('amount_threshold', 5)
    min_score = kwargs.get('min_score', 90)

    # Calculate the number of days to aggregate
    nbdays_agreges = int(nb_days_period / 2)
    step = 5
    for nb_days in np.arange(10, nbdays_agreges + step, step=step):
        clientname_col_bo = 'subscriber_name'
        payment_id_bo = 'order_id'
        date_colname_bo = 'creation_date'
        amount_colname_bo = 'total_amount'

        # Aggregate BO orders by date interval
        df_BO_agg = aggregate_by_date(df_BO, payment_id_bo, clientname_col_bo, date_colname_bo, amount_colname_bo, nb_days)
        if len(df_BO_agg) > 0:
            df_BO_agg['Start_Date'] = df_BO_agg['creation_date']
            df_BO_agg['End_Date'] = df_BO_agg['creation_date'] + dt.timedelta(days=nb_days_period)

            for clientname_col in list_cols_Clientname:
                # Match with aggregated BO data
                df_match = rapprocher_paiement_bo_basic(
                    df_payment, df_BO_agg, clientname_col, date_colname, amount_colname, payment_id,
                    amount_threshold, min_score
                )
                df_BO_agg = df_BO_agg[~df_BO_agg['order_id'].isin(df_match['order_id'])]

                # Revert to unit orders
                list_col = list(df_payment.columns)
                list_col.append('order_id')
                df_match = df_match[list_col]
                df_match['order_id'] = df_match['order_id'].str.split('|')
                df_match = df_match.explode('order_id', ignore_index=True)
                df_match = df_match.merge(df_BO, on='order_id')
                df_match['motif'] = motif
                df_rapproche = pd.concat([df_rapproche, df_match])
                df_payment = df_payment[~df_payment[payment_id].isin(df_rapproche[payment_id])]
                df_BO = df_BO[~df_BO['order_id'].isin(df_rapproche['order_id'])]

    return df_rapproche, df_payment, df_BO

def mapping_npeople(
    df_rapproche: pd.DataFrame,
    df_payment: pd.DataFrame,
    df_BO: pd.DataFrame,
    list_cols_Clientname: List[str],
    **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    This function matches contracts involving multiple people. It concatenates the columns 
    subscriber_name and cosubscriber_name to have one order with two different lines for 
    two different people. It then matches these lines with the payment, summing the payments 
    to compare the amount because each person is supposed to pay part of the order amount.

    Parameters:
    - df_rapproche (pd.DataFrame): DataFrame containing matched records.
    - df_payment (pd.DataFrame): DataFrame containing client payment data (check or transfer table).
    - df_BO (pd.DataFrame): BO orders/contracts not yet processed.
    - list_cols_Clientname (List[str]): List of columns regarding the payer in the transfer/check table.
    - kwargs: Additional optional parameters for matching:
        - date_colname (Optional[str]): Name of the Date column in the payment table.
        - amount_colname (Optional[str]): Name of the Amount column in the payment table.
        - payment_id (Optional[str]): Name of the system ID column (unique ID) in the payment table.
        - motif (Optional[str]): The method of matching to flag in the final result.
        - nb_days_period (int): Maximum number of days accepted between the creation date of the orders and the payment date.
        - amount_threshold (float): Acceptance threshold on the amount for matching.
        - min_score (int): Minimum accepted percentage for the similarity score.
        - is_bo (bool): Flag to indicate if the matching involves BO data.

    Returns:
    - Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: Updated DataFrames for matched records, remaining payments, and remaining BO orders.
    """
    # Extract keyword arguments with defaults
    date_colname = kwargs.get('date_colname')
    amount_colname = kwargs.get('amount_colname')
    payment_id = kwargs.get('payment_id')
    motif = kwargs.get('motif')
    nb_days_period = kwargs.get('nb_days_period', 2)
    amount_threshold = kwargs.get('amount_threshold', 5)
    min_score = kwargs.get('min_score', 90)
    is_bo = kwargs.get('is_bo', True)

    if is_bo: 
        # Filter BO contracts involving two people
        df_BO_2pp = df_BO[(~df_BO['cosubscriber_name'].isnull()) & ~df_BO['subscriber_name'].isnull()]
        df_BO_2PP_1 = df_BO_2pp[['order_id', 'product_code', 'total_amount', 'creation_date', 'subscriber_name']]
        df_BO_2PP_2 = df_BO_2pp[['order_id', 'product_code', 'total_amount', 'creation_date', 'cosubscriber_name']].rename(columns={
            'cosubscriber_name': 'subscriber_name'
        })
        df_BO_2pp = pd.concat([df_BO_2PP_1, df_BO_2PP_2])

        # Add Start_Date and End_Date columns
        df_BO_2pp['Start_Date'] = df_BO_2pp['creation_date']
        df_BO_2pp['End_Date'] = df_BO_2pp['creation_date'] + dt.timedelta(days=nb_days_period)

        # Match with date and product only
        df_match = mapping_approximately(df_payment, df_BO_2pp, amount_colname, date_colname, amount_threshold=-1000)
    else:
        df_match = mapping_approximately(df_payment, df_BO, amount_colname, date_colname, amount_threshold=-1000)

    df_match = df_match[~df_match['order_id'].isnull()]
    df_match = df_match[~df_match[payment_id].isnull()]

    if len(df_match) > 0:
        # Find the correct person with the score
        df_match = check_name(df_match, list_cols_Clientname, min_score)
        df_match = df_match[['order_id', 'total_amount', amount_colname, payment_id]].drop_duplicates()

        # Compare the amount
        df_match_agg = df_match.groupby(by=['order_id', 'total_amount']).agg({
            amount_colname: 'sum',
            payment_id: '|'.join
        }).reset_index()
        df_match_agg['ecart_montant'] = abs(df_match_agg['total_amount'] - df_match_agg[amount_colname])
        df_match_agg = df_match_agg[df_match_agg['ecart_montant'] <= amount_threshold]

        # Re-merge with the tables to get all information
        df_match_agg[payment_id] = df_match_agg[payment_id].str.split('|')
        df_match_agg = df_match_agg.explode(payment_id, ignore_index=True)
        df_match_agg = df_match_agg[['order_id', payment_id]]
        df_match = df_match_agg.merge(df_payment, on=payment_id)
        df_match = df_match.merge(df_BO, on='order_id')
        df_match['motif'] = motif
        df_rapproche = pd.concat([df_rapproche, df_match])
        df_BO = df_BO[~df_BO['order_id'].isin(df_match['order_id'])]
        df_payment = df_payment[~df_payment[payment_id].isin(df_match[payment_id])]

    return df_rapproche, df_payment, df_BO

def mapping_lightcheck_uniquepayment(
    df_rapproche: pd.DataFrame, 
    df_payment: pd.DataFrame, 
    df_BO: pd.DataFrame, 
    list_cols_Clientname: List[str],
    **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    This function performs a less strict matching rule: it checks if there is a common client name 
    between the payment and the BO.

    Parameters:
    - df_rapproche (pd.DataFrame): DataFrame containing matched records.
    - df_payment (pd.DataFrame): DataFrame containing client payment data (check or transfer table).
    - df_BO (pd.DataFrame): BO orders/contracts not yet processed.
    - list_cols_Clientname (List[str]): List of columns regarding the payer in the transfer/check table.
    - kwargs: Additional optional parameters for matching:
        - date_colname (Optional[str]): Name of the Date column in the payment table.
        - amount_colname (Optional[str]): Name of the Amount column in the payment table.
        - payment_id (Optional[str]): Name of the system ID column (unique ID) in the payment table.
        - motif (Optional[str]): The method of matching to flag in the final result.
        - amount_threshold (float): Acceptance threshold on the amount for matching.
        - bo_name_col (str): Column name in the BO table for the subscriber name.

    Returns:
    - Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: Updated DataFrames for matched records, remaining payments, and remaining BO orders.
    """
    # Extract keyword arguments with defaults
    date_colname = kwargs.get('date_colname')
    amount_colname = kwargs.get('amount_colname')
    payment_id = kwargs.get('payment_id')
    motif = kwargs.get('motif')
    amount_threshold = kwargs.get('amount_threshold', 5)
    bo_name_col = kwargs.get('bo_name_col', 'subscriber_name')

    # Loop through each client name column to perform light check matching
    for clientname_col in list_cols_Clientname:
        # Create light check matches
        df_match = create_light_check(df_payment, df_BO, clientname_col, date_colname,
                                      amount_colname, payment_id, amount_threshold, bo_name_col)
        df_match['motif'] = motif  # Add motif to matched DataFrame
        
        # Concatenate matched records to df_rapproche
        df_rapproche = pd.concat([df_rapproche, df_match])
        
        # Filter out matched records from df_payment and df_BO
        df_payment = df_payment[~df_payment[payment_id].isin(df_rapproche[payment_id])]
        df_BO = df_BO[~df_BO['order_id'].isin(df_rapproche['order_id'])]

    return df_rapproche, df_payment, df_BO



