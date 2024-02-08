import datetime as dt
import pandas as pd
import numpy as np
from utils.utils import *

def clean_check_rejects_mt940(df_rejet_cheque:pd.DataFrame):
    '''Get references from transaction_details for the case when only one check rejected by rejection.
    '''
    ### get date and check number from transaction_details
    patern_fr = r'(?<=FR)\/\d*\/(.*)' 
    df_rejet_cheque.loc[:,"motif_rejet"]  = df_rejet_cheque.loc[:,'transaction_details'].str.extract(patern_fr)
    df_rejet_cheque.loc[:,"motif_rejet"] = df_rejet_cheque.loc[:,"motif_rejet"].str.strip()
    df_rejet_cheque['numero_cheque_rejet'] = df_rejet_cheque.motif_rejet.str.split(' ').str[0]
    df_rejet_cheque['numero_cheque_rejet'] = df_rejet_cheque['numero_cheque_rejet'].apply(clean_num_cheque)

    patern_remise = r'(?<=REMISE)\s+(\d+)'
    df_rejet_cheque['numero_remise'] = df_rejet_cheque.motif_rejet.str.extract(patern_remise)
    df_rejet_cheque['numero_remise'] = df_rejet_cheque['numero_remise'].str.strip()
    df_rejet_cheque['numero_remise'] = df_rejet_cheque['numero_remise'].apply(clean_num_cheque)
    return df_rejet_cheque

