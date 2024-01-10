import pandas as pd
import sqlite3
from utils.utils import *
from mapping_transfer_check.duplicates_functions import *

## Ces fonctions appliquent le rapprochement sur une colonne de payeur donné VS les données BO

def mapping_approximately(
    df_payment: pd.DataFrame,
    df_BO: pd.DataFrame,
    amount_colname: str = None,
    date_colname: str = None,
    amount_threshold: float = 5.0,
    **kwargs
) -> pd.DataFrame:
    """
    This function maps payments to orders approximately based on the date and amount.
    
    Parameters:
    - df_payment (pd.DataFrame): DataFrame containing payment data.
    - df_BO (pd.DataFrame): DataFrame containing BO orders/contracts.
    - amount_colname (Optional[str]): Column name for amounts in the payment table.
    - date_colname (Optional[str]): Column name for dates in the payment table.
    - amount_threshold (float): Threshold for the difference in amount. Default is 5.0.
    - kwargs: Additional optional parameters.

    Returns:
    - pd.DataFrame: DataFrame containing matched payments and orders.
    """
    with sqlite3.connect(':memory:') as conn:
        # Write DataFrames to SQL tables
        df_payment.to_sql('releve', conn, index=False)
        df_BO.to_sql('BO', conn, index=False)
        
        # Construct base query for matching dates
        base_query = f"""
            SELECT *
            FROM releve
            LEFT JOIN BO ON (
                releve.{date_colname} BETWEEN BO.Start_Date AND BO.End_Date
        """
        
        # Add amount condition to the query
        if amount_threshold != -1000:
            query = f"""
                {base_query}
                AND ABS(releve.{amount_colname} - BO.total_amount) <= ?
            )
            """
            # Execute query with amount threshold parameter
            df_match = pd.read_sql_query(query, conn, params=[amount_threshold])
        else:
            query = f"""
                {base_query}
                AND releve.{amount_colname} <= BO.total_amount
            )
            """
            # Execute query without amount threshold parameter
            df_match = pd.read_sql_query(query, conn)
    
    return df_match



def rapprocher_paiement_bo_basic(
    df_payment: pd.DataFrame,
    df_rebo_ordre_vir: pd.DataFrame,
    clientname_col: str = None,
    date_colname: str = None,
    amount_colname: str = None,
    payment_id: str = None,
    amount_threshold: float = 5,
    min_score: int = 90,
    **kwargs
) -> pd.DataFrame:
    """
    This function performs a basic reconciliation where 1 payment = 1 order.
    Input columns are from the df_payment table, as the BO columns remain unchanged.
    The acceptance threshold for the amount and date difference can be modified.
    The similarity score for names should be >= 90%.

    Parameters:
    - df_payment (pd.DataFrame): DataFrame containing payment data.
    - df_rebo_ordre_vir (pd.DataFrame): DataFrame containing BO orders/contracts.
    - clientname_col (str): Column name for client names in the payment table.
    - date_colname (str): Column name for dates in the payment table.
    - amount_colname (str): Column name for amounts in the payment table.
    - payment_id (str): Column name for payment IDs in the payment table.
    - amount_threshold (float): Acceptance threshold for the amount difference.
    - min_score (int): Minimum similarity score for name matching.
    - kwargs: Additional optional parameters for the mapping functions.

    Returns:
    - pd.DataFrame: DataFrame containing matched payments and orders.
    """
    # Step 1: SQL code to take transactions with the same amount and close dates
    df_match = mapping_approximately(df_payment, df_rebo_ordre_vir, amount_colname, date_colname, amount_threshold)

    # Step 2: Calculate the fuzzy score on the obtained result
    df_match = df_match[~df_match[clientname_col].isnull() & ~df_match['subscriber_name'].isnull()]
    df_match = df_match[df_match[clientname_col].str.len() >= 4]

    if len(df_match) > 0:
        df_match = calculate_fuzz_score(df_match, clientname_col, 'subscriber_name')
        df_match = df_match[df_match["max_score"] >= min_score].reset_index(names='id_unique')

        # Step 3: Handle duplicates
        # 3.1 Separate duplicates
        mask1 = df_match.duplicated(subset=payment_id, keep=False)  # Duplicates on payments
        df_duplicates1 = df_match[mask1]
        mask2 = df_match.duplicated(subset='order_id', keep=False)  # Duplicates on orders
        df_duplicates2 = df_match[mask2]
        df_duplicates = pd.concat([df_duplicates1, df_duplicates2]).drop_duplicates(subset='id_unique')
        df_ok = df_match[~df_match.id_unique.isin(df_duplicates.id_unique)]

        # 3.2 Merge duplicates
        columns_left = list(df_payment.columns)
        columns_right = list(df_rebo_ordre_vir.columns)
        id_left = payment_id
        id_right = 'order_id'
        df_merge = merge_duplicates_by_date(df_duplicates, columns_left, columns_right, id_left, id_right, amount_colname, date_colname, clientname_col)

        # 3.3 Concatenate the result
        if len(df_merge) > 0:
            df_match = pd.concat([df_merge, df_ok])
        else:
            df_match = df_ok

        df_match = df_match.drop(columns=['id_unique', 'max_score'])

    return df_match

def create_light_check(
    df_payment: pd.DataFrame,
    df_BO: pd.DataFrame,
    clientname_col: str= None,
    date_colname: str = None,
    amount_colname: str = None,
    payment_id: str = None,
    amount_threshold: float = 5.0,
    bo_name_col: str = 'subscriber_name',
    **kwargs
) -> pd.DataFrame:
    """
    Perform a light check by matching payments and BO orders based on date and amount,
    and then refine the matches based on common words in client names.
    
    Parameters:
        df_payment (pd.DataFrame): DataFrame containing payment data.
        df_BO (pd.DataFrame): DataFrame containing BO order data.
        clientname_col (Optional[str]): Column name for client name in df_payment.
        date_colname (Optional[str]): Column name for date in df_payment.
        amount_colname (Optional[str]): Column name for amount in df_payment.
        payment_id (Optional[str]): Column name for payment ID in df_payment.
        amount_threshold (float): Threshold amount for matching. Default is 5.0.
        bo_name_col (str): Column name for subscriber name in df_BO. Default is 'subscriber_name'.
        kwargs: Additional optional parameters.
    
    Returns:
        pd.DataFrame: DataFrame containing the matched records.
    """
    
    # Perform approximate mapping based on date and amount
    df_match = mapping_approximately(df_payment, df_BO, amount_colname, date_colname, amount_threshold, **kwargs)
    
    # Filter matches to find common client names
    mask = (~df_match['order_id'].isnull()) & (~df_match[clientname_col].isnull())
    df_match = df_match.loc[mask, :]
    
    if not df_match.empty:
        df_match['nom_commun'] = df_match.apply(lambda f: find_commun_word(f[clientname_col], f[bo_name_col]), axis=1)
        df_match = df_match[df_match['nom_commun'] == True].reset_index(names='id_unique')

        # Handle duplicates
        df_duplicates1 = df_match[df_match.duplicated(subset=payment_id, keep=False)]
        df_duplicates2 = df_match[df_match.duplicated(subset='order_id', keep=False)]
        df_duplicates = pd.concat([df_duplicates1, df_duplicates2]).drop_duplicates(subset='id_unique')
        
        df_ok = df_match[~df_match['id_unique'].isin(df_duplicates['id_unique'])]
        df_duplicates_matched = pd.DataFrame()
        
        for i in range(5):  # There are max 5 duplicate payments
            df_merge = merge_duplicates_by_date(
                df_duplicates,
                list(df_payment.columns),
                list(df_BO.columns),
                payment_id,
                'order_id',
                amount_colname,
                date_colname,
                clientname_col,
                **kwargs
            )
            df_duplicates_matched = pd.concat([df_duplicates_matched, df_merge])
            df_duplicates = df_duplicates[~df_duplicates['order_id'].isin(df_duplicates_matched['order_id'])]
            df_duplicates = df_duplicates[~df_duplicates[payment_id].isin(df_duplicates_matched[payment_id])]
        
        df_match = pd.concat([df_duplicates_matched, df_ok])
    else:
        df_match = pd.DataFrame()
    
    return df_match

