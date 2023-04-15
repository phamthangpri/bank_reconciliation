import datetime as dt
import pandas as pd

from utils.clean_reference import *
from utils.utils import *
from utils.clean_check_reject import *
from utils.clean_check import *
from utils.clean_reference import *


def clean_data_mt940(df_releve: pd.DataFrame, entity: str) -> dict:
    """
    Clean and categorize transactions from MT940 bank statement data.

    Parameters:
    df_releve (pd.DataFrame): The bank statement data.
    entity (str): The entity type, used to filter transactions.

    Returns:
    dict: A dictionary containing categorized transaction DataFrames.
    """
    
    # Filter transactions for France
    df_releve_fr = df_releve[df_releve.country=='FRANCE']

    # Filter for transfers, excluding specific types
    df_virement = df_releve_fr[(df_releve_fr.transaction_type.str.contains('Virement',na=False)) & (df_releve.sense == 'C')]
    df_virement = df_virement[~df_virement.transaction_details.astype(str).str.contains('PARRAINAGE')]
    if entity != 'ABCD':
        mask = (~df_virement.reference1.astype(str).str.contains('COMPANY|CESSION',na=False)) & \
        (~df_virement.reference1.isnull()) & \
        (~df_virement.extra_information.astype(str).str.contains('DIVIDEND| PD1 | PD2| PD3 | SHORTTERM BO|OPCVM',na=False)) 
        df_virement = df_virement.loc[mask,:]

    # Filter for cheque deposits
    df_releve_cheque = df_releve_fr[df_releve_fr.transaction_type.str.contains('remise de chèques',na=False)]
    df_releve_cheque = clean_check_mt940(df_releve_cheque,entity)

    # Filter for direct debits
    mask = (df_releve_fr.transaction_type.str.contains('Prélèvements',na=False)) & (df_releve_fr.sense == 'C')
    df_releve_prlv = df_releve_fr.loc[mask,:].drop(columns = ['clientname', 'reference1','reference2'])

    # Identify rejected transactions
    df_debit = df_releve_fr[df_releve_fr.sense == 'D']
    df_rejet = df_debit[df_debit.transaction_type=='Rejected / Not paid']

    mask = df_rejet.transaction_details.str.contains('CHECK CANCELED|CHECK NOT PAID')
    df_rejet_cheque = df_rejet[mask]
    df_rejet_prlv = df_rejet[~mask]
    dict_result = {
                'Transfer'   : df_virement,
                'Check'      : df_releve_cheque,
                'Direct_debit' : df_releve_prlv,
                'Check_rejected': df_rejet_cheque,
                'Direct_debit_rejected'  : df_rejet_prlv
               }
    return dict_result

def clean_data_check(df_cheque:pd.DataFrame):
    df_cheque['Product'] = df_cheque['Receiver'].map({  'PRODUIT1':'PD1', 
                                                        'PRODUIT2':'PD2', 
                                                        'PRODUIT3':'PD3', 
                                                        'PRODUIT4':'PD4'})
    df_cheque['check_holder'] = df_cheque['check_holder'].apply(clean_name)
    df_cheque['check_number'] = df_cheque['check_number'].apply(clean_num_cheque)
    if 'doc_num' in df_cheque.columns:
        df_cheque['doc_num'] = df_cheque['doc_num'].apply(clean_num_cheque)
    if 'ord_num' in df_cheque.columns:
        df_cheque = df_cheque[(df_cheque.NuméroOrdre.isnull()) & (df_cheque.DateReception>='2023-01-01')]
    return df_cheque

def clean_data_BO(df_mapping_col:pd.DataFrame,df_BO:pd.DataFrame,entity:str):
    ## Renommer les colonnes
    dict_name = dict(zip(df_mapping_col[entity],df_mapping_col['column']))
    df_BO = df_BO.rename(columns=dict_name)

    ### Enlever les M et Mme dans le nom
    df_BO.subscriber_name = df_BO.subscriber_name.apply(clean_name)
    df_BO.cosubscriber_name = df_BO.cosubscriber_name.apply(clean_name)
    df_BO.order_id = df_BO.order_id.astype(str) 

    ### Splitter en virements et chèque
    if entity == 'ABCD':
        df_BO = df_BO[~df_BO.order_status.isin(['Cancelled','Refused'])]
        df_BO_chq = df_BO[df_BO.payment_mode=='Check']
        df_BO_vir = df_BO[df_BO.payment_mode.isin(['Funding', 'Transfer'])]
    else:
        df_BO_chq = df_BO[df_BO.payment_mode=='Bank check']
        df_BO_vir = df_BO[df_BO.payment_mode.isin(['Direct Transfer'])]
    return df_BO_vir,df_BO_chq
