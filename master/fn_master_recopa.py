import pandas as pd

from master_project.clean_data import *
from mapping_transfer_check.master_functions import *
from mapping_reject.check_rejection_functions import *
from mapping_reject.direct_debit_rejection import *
from mapping_direct_debit.direct_debit_control import *


def master_project(entity: str, df_mapping_col: pd.DataFrame,
                  df_releve: pd.DataFrame, df_cheque: pd.DataFrame, 
                  df_BO: pd.DataFrame, df_prlv_sub: pd.DataFrame):
    '''
    Master function that handles all reconciliations for the project.
    Steps:
        1. Clean input data (names, check numbers, etc.) for MT940, check deposits, BO
        2. Reconcile bank transfers
        3. Reconcile checks
        4. Perform checks on direct debits
        5. Reconcile unpaid checks

    Args:
        entity (str): The entity name, e.g., 'ABCD' or 'XYZ'.
        df_mapping_col (pd.DataFrame): Column mapping for BO.
        df_releve (pd.DataFrame): MT940 data.
        df_cheque (pd.DataFrame): Check deposit data.
        df_BO (pd.DataFrame): BO data.
        df_prlv_sub (pd.DataFrame): Direct debit XML data.

    Returns:
        dict: A dictionary where the keys are the names of the reconciliation steps 
              and the values are the corresponding results.
    '''
    
    dict_result_project = {}
    
    # Step 1: Clean MT940 data
    dict_result = clean_data_mt940(df_releve, entity)
    df_virement = dict_result['Virement']
    df_releve_cheque = dict_result['Cheque']  # To be added: check step for id_sys
    df_releve_prlv = dict_result['Prelevement']
    df_rejet_cheque = dict_result['Rejet_cheque']
    df_rejet_prlv = dict_result['Rejet_prlv']

    # Step 2: Clean check deposit data
    df_cheque = clean_data_check(df_cheque)

    # Step 3: Clean BO data
    df_BO_vir, df_BO_chq = clean_data_BO(df_mapping_col, df_BO, entity)

    # Step 4: Reconcile credit lines
    if entity == 'ABCD': 
        column_name_bo = 'subscriber_name'
        amount_threshold = 5
    else: 
        column_name_bo = 'client_lastname'
        amount_threshold = 0
    
    dict_nb_days = {
        'ABCD_PP': 60,
        'ABCD_Démembrement': 180,
        'XYZ': 20
    }
    min_score = 90

    # 4.1 Reconcile transfers
    if len(df_virement) > 0:
        print('RECONCILIATION OF TRANSFERS')
        print(f'Number of transfer lines to process: {len(df_virement)}')
        client_payment_columns = ['reference1', 'reference2', 'clientname']
        date_column, amount_column, payment_id = 'effective_date', 'amount', 'id_sys'
        columns_to_keep = ['transaction_details', 'iban_product']
        df_virement_final = master_mapping_transfer_check(
            entity, df_virement, df_BO_vir, df_mapping_col, columns_to_keep,
            client_payment_columns, date_column, amount_column, payment_id,
            amount_threshold, min_score, column_name_bo, dict_nb_days
        )
        dict_result_project['virement'] = df_virement_final

    # 4.2 Reconcile checks
    if len(df_cheque) > 0:
        print('RECONCILIATION OF CHECKS')
        df_cheque = df_cheque.rename(columns={'Produit': 'iban_product'})
        columns = df_mapping_col[~df_mapping_col[entity].isnull()]['column'].tolist()
        df_BO_chq = df_BO_chq[columns]
        if entity == 'ABCD':
            df_cheque = df_cheque[df_cheque['iban_product'].isin(['PD1', 'PD2', 'PD3'])]
        else:
            df_cheque = df_cheque[~df_cheque['iban_product'].isin(['PD1', 'PD2', 'PD3'])]

        print(f'Number of checks to reconcile: {len(df_cheque)},\n' +
              f'Number of BO checks to reconcile: {len(df_BO_chq)}')

        client_payment_columns = ['Titulaire']
        date_column, amount_column, payment_id = 'DateReception', 'Montant', 'id_cheque'
        columns_to_keep = ['iban_product', 'NumCheque', 'Nbordereau', 'DateCheque']
        df_cheque_final = master_mapping_transfer_check(
            entity, df_cheque, df_BO_chq, df_mapping_col, columns_to_keep,
            client_payment_columns, date_column, amount_column, payment_id,
            amount_threshold, min_score, column_name_bo, dict_nb_days
        )
        df_cheque_final = df_cheque_final.rename(columns={'iban_product': 'Destinataire'})
        dict_result_project['cheque'] = df_cheque_final

    # 4.3 Verify direct debit data
    if len(df_releve_prlv) > 0:
        df_prlv_sub.session_id = df_prlv_sub.session_id.str.upper()
        df_result_prlv = check_direct_debit(df_prlv_sub, df_releve_prlv, entity)
        dict_result_project['prelevement'] = df_result_prlv

    # Step 5: Reconcile unpaid checks
    if len(df_rejet_cheque) > 0:
        checknum_column, checkamount_column, checkdate_column, checkproduct_column = 'NumCheque', 'Montant', 'DateCheque', 'Destinataire'
        df_merge_check_rejects = master_mapping_check_rejection(
            df_rejet_cheque, df_cheque, checknum_column, checkamount_column, checkdate_column, checkproduct_column
        )
        dict_result_project['cheque_impaye'] = df_merge_check_rejects

    return dict_result_project
