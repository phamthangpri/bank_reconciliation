import datetime as dt
import pandas as pd
import sqlite3
from utils.utils import *

def aggregate_by_date(df_payment: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Aggregates all transactions within a specified number of days for a person for each date.

    Parameters:
    - df_payment: DataFrame containing payment information.
    - *args: Additional positional arguments.
    - **kwargs: Additional keyword arguments. Must include:
        - payment_id: Unique identifier for the payment.
        - clientname_col: Column name for the client's name.
        - date_colname: Column name for the date.
        - amount_colname: Column name for the amount.
        - nb_days: Number of days interval to aggregate (default 2).

    Returns:
    - DataFrame with aggregated payments.
    """
    payment_id = kwargs.get('payment_id')
    clientname_col = kwargs.get('clientname_col')
    date_colname = kwargs.get('date_colname')
    amount_colname = kwargs.get('amount_colname')
    nb_days = kwargs.get('nb_days', 2)
    
    # Etape 1: aggregate transfers from the same client and same date to avoid creating duplicates on the next steps
    ### ie concatenate paiement1 and paiement2 => paiement12
    df_payment = df_payment[(df_payment[clientname_col]!='') & (~df_payment[clientname_col].isnull())]
    if len(df_payment)>0:
        df_payment_agrege = df_payment.groupby(by=[clientname_col,date_colname]).\
                                                    agg({amount_colname:'sum',\
                                                        payment_id : "|".join   # concaténer les Id sys pour la même personne
                                                        }).reset_index()

        #Etape 2: looking for the transfers during a period of  x days (nb_days)
        # 2.1 add period
        df1 = df_payment_agrege[[payment_id,clientname_col,date_colname,amount_colname]].sort_values(by=[clientname_col,date_colname])
        df2 = df1.copy()
        payment_id_2 = payment_id +"_2"
        clientname_col_2 = clientname_col +"_2"
        date_colname_2 = date_colname +"_2"
        amount_colname_2 = amount_colname+"_2"
        df2.columns = [payment_id_2,clientname_col_2 ,date_colname_2,amount_colname_2]

        df1["End_date"] = df1[date_colname]+dt.timedelta(days=+int(nb_days))

        # 2.2 flag all transfers in that period
        conn = sqlite3.connect(':memory:')
        df1.to_sql('df1', conn, index=False)
        df2.to_sql('df2', conn, index=False)
        qry = f'''
            SELECT * 
            FROM df1 
            JOIN df2 
            ON df1.{clientname_col} = df2.{clientname_col_2}
            AND df2.{date_colname_2} <= df1.End_date 
            AND df2.{date_colname_2} > df1.{date_colname}
        '''

        df_match_date_sup = pd.read_sql_query(qry,conn)

        #2.3 remove all payments already taken in the period of x days from the start date
        ### it will create 3 rows : paiement12 et paiement3, paiement12 et paiement4, paiement3 and paiement4
        ### needs to remove paiement3 et paiement4
        list_idsys2 = set(list(df_match_date_sup[payment_id_2])) # list des Id sys qui sont déjà utilisés
        list_idsys_agreges = set(list(df_match_date_sup[payment_id_2])+list(df_match_date_sup[payment_id])) # tous les id sys
        df_match_date_sup = df_match_date_sup[~df_match_date_sup[payment_id].isin(list_idsys2)] # enlever les lignes déjà utilisés
        list_idsys = set(list(df_match_date_sup[payment_id])) #+list_concat_meme_date

        # 2.4 add transfer data to the start date
        ### add paiement12 and paiement12 to make groupby
         # Add initial transactions
        df_date0_to_add = df_payment_agrege[df_payment_agrege[payment_id].isin(list_idsys)] 
        df_date0_to_add[payment_id_2] = df_date0_to_add[payment_id]
        df_date0_to_add[amount_colname_2] = df_date0_to_add[amount_colname]

        ### records on people who didn't make several transfers or who did 2 transfers in a day
        df_not_agrege = df_payment_agrege[~df_payment_agrege[payment_id].isin(list_idsys_agreges)]
        df_not_agrege[payment_id_2] = df_not_agrege[payment_id]
        df_not_agrege[amount_colname_2] = df_not_agrege[amount_colname]

        # 2.5 : aggregate data
        ### df_match_date_sup :  paiement12 et paiement3, paiement12 et paiement4
        ### df_date0_to_add :  paiement12 et paiement12
        ### df_not_agrege : lignes unitaires
        df_match_date_sup = pd.concat([df_match_date_sup,df_date0_to_add,df_not_agrege]).sort_values([clientname_col,date_colname])
        df_match_date_sup[date_colname] = pd.to_datetime(df_match_date_sup[date_colname])

        # Prendre la date
        df_match_date_sup[date_colname_2] = df_match_date_sup.apply(lambda f : get_date(f[payment_id],
                                                                                f[payment_id_2],
                                                                                f[date_colname],
                                                                                f[date_colname_2]), axis=1 )
        df_match_date_sup[date_colname_2] = pd.to_datetime(df_match_date_sup[date_colname_2],format = '%Y-%m-%d %H:%M:%S')
        df_match_date_sup = df_match_date_sup.groupby(by=[payment_id,date_colname]).agg({amount_colname_2:"sum",
                                                                    clientname_col:'first',
                                                                    payment_id_2: "|".join,
                                                                    date_colname_2:"max"
                                                                }).reset_index()
        df_match_date_sup[payment_id] = df_match_date_sup[payment_id].apply(lambda x: x.split("|")[0] if "|" in x else x)
        df_match_date_sup = df_match_date_sup[[payment_id_2,date_colname,clientname_col,amount_colname_2,date_colname_2]]
        df_match_date_sup = df_match_date_sup.rename(columns={payment_id_2:payment_id,amount_colname_2:amount_colname,date_colname_2:'Max_'+date_colname})
        df_match_date_sup[amount_colname+"_total"] = df_match_date_sup[amount_colname]
    else : df_match_date_sup = pd.DataFrame()
    return df_match_date_sup


def merge_duplicates_by_date(df_duplicates: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    '''Merges the payment table and BO table with duplicates.
    
    This handles cases where a client makes multiple consecutive payments for several orders over close months.
    It takes the oldest payments for the oldest orders.
    
    Parameters:
    - df_duplicates: DataFrame containing duplicates.
    - *args: Additional positional arguments.
    - **kwargs: Additional keyword arguments. Must include:
        - columns_left: List of columns for the left DataFrame.
        - columns_right: List of columns for the right DataFrame.
        - id_left: Unique identifier column for the left DataFrame.
        - id_right: Unique identifier column for the right DataFrame.
        - amount_colname: Amount column name (default 'amount').
        - date_colname: Date column name (default 'effective_date').
        - clientname_col: Client name column name (default 'clientname').
        - amount_threshold: Threshold amount for matching (default 5).

    Returns:
    - Merged DataFrame.
    '''
    columns_left = kwargs.get('columns_left')
    columns_right = kwargs.get('columns_right')
    id_left = kwargs.get('id_left')
    id_right = kwargs.get('id_right')
    amount_colname = kwargs.get('amount_colname', 'amount')
    date_colname = kwargs.get('date_colname', 'effective_date')
    clientname_col = kwargs.get('clientname_col', 'clientname')
    amount_threshold = kwargs.get('amount_threshold', 5)
    
    # Create combo name column
    df_duplicates.loc[:,'nompaiement_nombo'] = df_duplicates[clientname_col] + '|' + df_duplicates.subscriber_name # créer la colonne combo des noms
    columns_left.append('nompaiement_nombo')
    columns_right.append('nompaiement_nombo')
    
    # Remove duplicates in each table
    df_left = df_duplicates[columns_left].drop_duplicates(subset=id_left) 
    df_right = df_duplicates[columns_right].drop_duplicates(subset=id_right)

    # Sort values
    df_left = df_left.sort_values(by=['nompaiement_nombo',date_colname,amount_colname])
    df_right = df_right.sort_values(by=['nompaiement_nombo','creation_date','total_amount'])
    
    # Create order by date
    df_left['index'] = df_left.groupby(["nompaiement_nombo",amount_colname]).cumcount()+1 
    df_right['index'] = df_right.groupby(["nompaiement_nombo",'total_amount']).cumcount()+1
   
    # Merge by order of date
    df_merge = df_left.merge(df_right,on=['index','nompaiement_nombo'])
    df_merge['ecart_montant'] = abs(df_merge.total_amount - df_merge[amount_colname])
    
    # Aggregate and filter by matching amount
    df_merge_agg = df_merge.groupby(by=['index',id_left])['ecart_montant'].min().reset_index()
    df_merge_agg = df_merge_agg[df_merge_agg.ecart_montant<=amount_threshold] 
    
    # Final merge
    df_merge = df_merge.merge(df_merge_agg,on=[id_left,'ecart_montant','index']).drop(columns=['index','nompaiement_nombo','ecart_montant'])
    return df_merge

def merge_with_duplicates(df1: pd.DataFrame,df2: pd.DataFrame,*args, **kwargs) -> pd.DataFrame:
    '''Merges two DataFrames handling duplicates in each.
    
    For example: a client makes 4 payments with the same amount on the same day.
    Normal merging would create 16 rows. This function maps row by row, 
    then eliminates this row from both data sets to continue matching.

    Parameters:
    - df1: First DataFrame.
    - df2: Second DataFrame.
    - *args: Additional positional arguments.
    - **kwargs: Additional keyword arguments. Must include:
        - df1_unique_columns: Unique columns in the first DataFrame.
        - df2_unique_columns: Unique columns in the second DataFrame.
        - left_on_: Column name to join on from the first DataFrame.
        - right_on_: Column name to join on from the second DataFrame.
        - on_: Column name to join on from both DataFrames.
        - nb_matching: Number of matching iterations (default 5).


    Returns:
    - A merged DataFrame with duplicates handled.
    '''
    df1_unique_columns = kwargs.get('df1_unique_columns')
    df2_unique_columns = kwargs.get('df2_unique_columns')
    left_on_ = kwargs.get('left_on_', '')
    right_on_ = kwargs.get('right_on_', '')
    on_ = kwargs.get('on_', '')
    nb_matching = kwargs.get('nb_matching', 5)
    
    df_match_concat = pd.DataFrame()
    df1 = df1.reset_index(names='id1')
    df2 = df2.reset_index(names='id2')
    for i in range(0,nb_matching): 
        df1_i = df1.drop_duplicates(subset=df1_unique_columns,keep='first')
        df1 = df1[~df1["id1"].isin(df1_i["id1"])]
        df2_i = df2.drop_duplicates(subset=df2_unique_columns,keep='first')
        df2 = df2[~df2["id2"].isin(df2_i["id2"])]
        if on_ == '':
            df_match = df1_i.merge(df2_i,left_on=left_on_,right_on=right_on_)
        else:
            df_match = df1_i.merge(df2_i,on=on_)
        df_match_concat = pd.concat([df_match_concat,df_match])
    df_match_concat = df_match_concat.drop(columns=['id1','id2'])
    return df_match_concat