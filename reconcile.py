import datetime as dt
import pandas as pd
import numpy as np

from master_project.clean_data import *
from mapping_transfer_check.master_functions import *
from mapping_reject.check_rejection_functions import *
from mapping_reject.direct_debit_rejection import *
from mapping_direct_debit.direct_debit_control import *


def master_project(entity: str, df_mapping_col: pd.DataFrame, df_releve: pd.DataFrame, 
                   df_cheque: pd.DataFrame, df_BO: pd.DataFrame, df_prlv_sub: pd.DataFrame):
    '''
    This is the master function that handles all reconciliations for the project:
    Steps include:
        + Clean input data (names, check number, etc.) for MT940, check deposit, BO
        + Reconcile transfers
        + Reconcile checks
        + Perform checks on direct debits
        + Reconcile bounced checks
    Args:
        + entity: ABCD or XYZ
        + df_mapping_col: column mapping for the BO
        + df_releve: MT940 data
        + df_cheque: check deposit data
        + df_BO: BO Data
        + df_prlv_sub: direct debit XML data
    Returns:
        Dictionary with the key as the step name and the value as the corresponding result
    '''
    dict_resultat_project = {}
    
    ############# Step 1: Select MT940 data: #############
    dict_result = clean_data_mt940(df_releve, entity)
    df_virement = dict_result['Virement']
    df_releve_cheque = dict_result['Cheque']  # To add the step releve_cheque vs remise_chèque to get id_sys
    df_releve_prlv = dict_result['Prelevement']
    df_rejet_cheque = dict_result['Rejet_cheque']
    df_rejet_prlv = dict_result['Rejet_prlv']

    ############# Step 2: Clean check deposit data #############
    df_cheque = clean_data_check(df_cheque)

    ############# Step 3: Clean BO data: #############
    df_BO_vir, df_BO_chq = clean_data_BO(df_mapping_col, df_BO, entity)
    
    ############# Step 4: Reconcile credit lines: #############
    if entity == 'ABCD':
        bo_name_col = 'client_name'
        amount_threshold = 5
    else:
        bo_name_col = 'sub_name'
        amount_threshold = 0

    dict_nb_jours = {
        'ABCD_PP': 60,
        'ABCD_Démembrement': 180,
        'XYZ': 20
    }
    min_score = 90
    
    ### 4.1 Reconcile transfers:
    if len(df_virement) > 0:
        print(f'RECONCILING TRANSFERS')
        print(f'Number of transfer lines to process: {len(df_virement)}')
        
        colonnes_nomClient_paiement = ['reference1', 'reference2', 'clientname']
        date_colname, amount_colname, id_paiement = 'effective_date', 'amount', 'id'
        col_paiements_a_garder = ['transaction_details', 'account_code']
        
        kwargs = {
            'date_colname': date_colname,
            'amount_colname': amount_colname,
            'id_paiement': id_paiement,
            'amount_threshold': amount_threshold,
            'min_score': min_score,
            'nb_days_period': dict_nb_jours,
            'bo_name_col': bo_name_col
        }
        
        df_virement_final = master_mapping_transfer_check(entity, df_virement, df_BO_vir, df_mapping_col, col_paiements_a_garder,
                                                          colonnes_nomClient_paiement, **kwargs)
        dict_resultat_project['virement'] = df_virement_final
    
    ### 4.2 Reconcile checks:
    if len(df_cheque) > 0:
        print(f'RECONCILING CHECKS')
        df_cheque = df_cheque.rename(columns={'Produit': 'account_num'})
        
        colonnes = df_mapping_col[~df_mapping_col[entity].isnull()]['column'].to_list()
        df_BO_chq = df_BO_chq[colonnes]
        
        if entity == 'ABCD':
            df_cheque = df_cheque[df_cheque['account_num'].isin(['PD1', '¨PD2', 'PD3'])]
        else:
            df_cheque = df_cheque[~df_cheque['account_num'].isin(['PD1', '¨PD2', 'PD3'])]
        
        print(f'Number of check lines to reconcile: {len(df_cheque)},\n'+
              f'Number of BO check lines to reconcile: {len(df_BO_chq)}')
        
        colonnes_nomClient_paiement = ['Titulaire']
        date_colname, amount_colname, id_paiement = 'DateReception', 'Montant', 'id_cheque'
        col_paiements_a_garder = ['account_num', 'NumCheque', 'Nbordereau', 'DateCheque']
        
        kwargs = {
            'date_colname': date_colname,
            'amount_colname': amount_colname,
            'id_paiement': id_paiement,
            'amount_threshold': amount_threshold,
            'min_score': min_score,
            'nb_days_period': dict_nb_jours,
            'bo_name_col': bo_name_col
        }
        
        df_cheque_final = master_mapping_transfer_check(entity, df_cheque, df_BO_chq, df_mapping_col, col_paiements_a_garder,
                                                        colonnes_nomClient_paiement, **kwargs)
        df_cheque_final = df_cheque_final.rename(columns={'account_num': 'Destinataire'})
        dict_resultat_project['cheque'] = df_cheque_final
    
    ### 4.3 Verify direct debit data:
    if len(df_releve_prlv) > 0:
        df_prlv_sub.session_id = df_prlv_sub.session_id.str.upper()
        df_resultat_prlv = check_direct_debit(df_prlv_sub, df_releve_prlv, entity)
        dict_resultat_project['prelevement'] = df_resultat_prlv
    
    ############ Step 5: Reconcile debits: #############
    if len(df_rejet_cheque) > 0:
        # To do: Take the list of checks from the BO for df_cheque when BO data is cleaner and up-to-date with automatic reconciliation
        # Replace the following columns with those from BO 
        checknum_column, checkamount_column, checkdate_column, checkproduct_column = 'check_number', 'check_amount', 'check_date', 'check_receiver'
        
        df_merge_check_rejects = master_mapping_check_rejection(df_rejet_cheque, df_cheque,
                                                                checknum_column, checkamount_column, checkdate_column, checkproduct_column)
        dict_resultat_project['check_notpaid'] = df_merge_check_rejects
    
    return dict_resultat_project
