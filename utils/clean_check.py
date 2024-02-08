import datetime as dt
import pandas as pd
import re
from utils.utils import *

def find_deposit_info(text:str = None, find_nb_check:bool = True):
    text = text.replace('NOPE','').replace('/','')
    if find_nb_check: 
        result = text.split()[0]
        result = re.findall(r'(\d+)[A-Z]+',result)
        if result : result = result[0]
        else : result = '0'
    else : result = text.split()[-1]
    return result

def clean_check_mt940(df_releve_cheque: pd.DataFrame, entity: str = 'ABCD', **kwargs) -> pd.DataFrame:
    """
    Cleans and processes MT940 cheque data based on the specified entity.
    
    Parameters:
        df_releve_cheque (pd.DataFrame): DataFrame containing cheque transaction details.
        entity (str): Entity identifier, default is 'ABCD'.
        kwargs: Additional optional parameters.
    
    Returns:
        pd.DataFrame: Cleaned DataFrame with processed cheque details.
    """
    if entity == 'ABCD':
        pattern_remise = r'(?<=NOPE)\/(.*)|(?<=FR)\/\d*\/+(.*)'
        
        df_result = df_releve_cheque['transaction_details'].str.extract(pattern_remise)
        df_releve_cheque["deposit_details"] = df_result[0].combine_first(df_result[1])
        df_releve_cheque["deposit_details"] = df_releve_cheque["deposit_details"].str.strip()

        # Get deposit_number
        mask = df_releve_cheque['deposit_details'].str.contains('BORDEREAU', na=False)
        df_releve_cheque.loc[mask, 'deposit_number'] = df_releve_cheque.loc[mask].apply(
            lambda f: find_deposit_info(f['deposit_details'], False), axis=1
        )

        # Get nb_checks in deposit
        mask = ~df_releve_cheque['deposit_details'].str.contains('BORDEREAU', na=False)
        df_releve_cheque.loc[mask, 'nb_checks'] = df_releve_cheque.loc[mask].apply(
            lambda f: find_deposit_info(f['deposit_details'], True), axis=1
        )

        # Get deposit_date
        df_releve_cheque.loc[mask, 'deposit_date'] = df_releve_cheque.loc[mask].apply(
            lambda f: find_deposit_info(f['deposit_details'], False), axis=1
        )
        df_releve_cheque.loc[mask, 'deposit_date'] = (
            df_releve_cheque.loc[mask, 'date'].dt.year.astype(str) + df_releve_cheque.loc[mask, 'deposit_date']
        )
        df_releve_cheque.loc[mask, 'deposit_date'] = pd.to_datetime(df_releve_cheque.loc[mask, 'deposit_date'], format='%Y%m%d')

        # Modify date if deposit_date is after the transaction date
        mask = (~df_releve_cheque['deposit_date'].isnull()) & (df_releve_cheque['deposit_date'] > df_releve_cheque['date'])
        df_releve_cheque.loc[mask, 'deposit_date'] = df_releve_cheque.loc[mask, 'deposit_date'] + pd.DateOffset(years=-1)
        
        df_releve_cheque = df_releve_cheque.drop(columns='deposit_details')
    else:
        df_releve_cheque['deposit_number'] = df_releve_cheque['extra_information'].str.split().str[-1]
        df_releve_cheque['deposit_number'] = df_releve_cheque['deposit_number'].apply(clean_num_cheque, **kwargs)
    
    return df_releve_cheque