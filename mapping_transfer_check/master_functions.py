import pandas as pd
import datetime as dt
from mapping_transfer_check.basic_functions import *
from mapping_transfer_check.different_types_mapping_functions import *

'''Params :
        + entity : ABCD / xyz
        + df_paiement : dataframe sur le paiement du client (table de chèque ou virement)
        + df_BO : ordres/contrats de BO pas encore traités
        + list_cols_clientname_payment : liste des colonnes sur le payeur de la table de virement / chèque
        + date_colname : nom de la colonne Date de la table de paiement
        + amount_colname : nom de la colonne Montant de la table de paiement
        + id_paiement : nom de la colonne id sysètme (id unique) de la table de paiement
        + amount_threshold : seuil d'acceptation sur le montant pour le rapprochement
        + min_score : pourcentage minimum accepté pour le score de similarité
        + bo_name_col : nom de la colonne Nom de client de la table BO
'''
kwargs = {
    'date_colname':'date_colname',
    'amount_colname':'amount_colname',
    'id_paiement':'id_paiement',
    'motif':'motif',
    'amount_threshold':'amount_threshold',
    'min_score':'min_score',
    'nb_days_period':'nb_days_period',
    'mapping_type':'mapping_type',
    'bo_name_col':'bo_name_col'
}

def mapping_paiement_bo(df_rapproche: pd.DataFrame,df_paiement_a_traiter: pd.DataFrame,df_BO_a_traiter: pd.DataFrame,
                        list_cols_clientname_payment:str,mapping_type:str=None,**kwargs):
    '''Cette fonction permet de rapprocher les paiements avec les ordres dans le BO.
    ça va rapprocher quelques soit si c'est du mauvais compte, paiement pour pls ordres ou pls ordres pour un paiement.
    '''
    df_paiement_restant = pd.DataFrame()
    if len(df_rapproche) == 0:
        df_rapproche['mauvais_compte']=''
    if len(df_paiement_a_traiter)>0:
        for product_code in df_paiement_a_traiter['iban_product'].unique():
            df_paiement = df_paiement_a_traiter[df_paiement_a_traiter['iban_product']==product_code]
            ### Spéraper la table BO en deux
            df_BO_fonds = df_BO_a_traiter[df_BO_a_traiter.product_code==product_code]
            df_BO_autre = df_BO_a_traiter[df_BO_a_traiter.product_code!=product_code]
            df_BO_a_traiter = pd.DataFrame()
            for df_BO in [df_BO_fonds,df_BO_autre]:
                if len(df_paiement['iban_product'].unique())==len(df_BO.product_code.unique()):
                    mauvais_compte=False
                else: mauvais_compte = True
                if mapping_type == 'basic':
                    #### 1. Paiement unique
                    motif = 'paiement_unique'
                    df_rapproche,df_paiement,df_BO = mapping_unique_payment(df_rapproche,df_paiement,df_BO,\
                                                                            list_cols_clientname_payment,**kwargs)

                    ### 2. Plusieurs paiements pour un ordre :
                    motif = 'pls_paiement_1ord'
                    df_rapproche,df_paiement,df_BO = mapping_npaiement_1ord(df_rapproche,df_paiement,df_BO,\
                                                                            list_cols_clientname_payment,**kwargs)     

                    ### 3. Un paiement pour plusieurs ordres

                    motif = '1paiement_pls_ord'
                    df_rapproche,df_paiement,df_BO = mapping_1paiement_nord(df_rapproche,df_paiement,df_BO,\
                                                                            list_cols_clientname_payment,**kwargs)    
                    mask = df_rapproche.mauvais_compte.isnull()
                    df_rapproche.loc[mask,'mauvais_compte'] = mauvais_compte
                elif mapping_type == 'pls_pp':
                    motif = 'souscription_pls_pp'
                    df_rapproche,df_paiement,df_BO = mapping_npeople(df_rapproche,df_paiement,df_BO,
                                                        list_cols_clientname_payment,is_bo=True,**kwargs)
                    mask = df_rapproche.mauvais_compte.isnull()
                    df_rapproche.loc[mask,'mauvais_compte'] = mauvais_compte
                elif mapping_type == 'pls_paiements_diff_motifs':
                    motif = 'pls_paiement_differents_motifs'
                    df_match,df_paiement,df_BO = mapping_npeople(df_rapproche,df_paiement,df_BO,
                                                    list_cols_clientname_payment,is_bo=False, **kwargs)
                    mask = df_rapproche.mauvais_compte.isnull()
                    df_rapproche.loc[mask,'mauvais_compte'] = mauvais_compte
                elif mapping_type == 'light_check_paiementunique':
                    motif = mapping_type
                    df_rapproche, df_paiement,df_BO = mapping_lightcheck_uniquepayment(df_rapproche,df_paiement,df_BO,
                                                        list_cols_clientname_payment,**kwargs)
                    mask = df_rapproche.mauvais_compte.isnull()
                    df_rapproche.loc[mask,'mauvais_compte'] = mauvais_compte
                elif mapping_type == 'light_check_pls_paiements_1ord':
                    motif = mapping_type
                    df_rapproche,df_paiement,df_BO = mapping_npaiement_1ord(df_rapproche,df_paiement,df_BO,\
                                                        list_cols_clientname_payment,is_lightcheck = True,**kwargs ) 
                    mask = df_rapproche.mauvais_compte.isnull()
                    df_rapproche.loc[mask,'mauvais_compte'] = mauvais_compte
                df_BO_a_traiter = pd.concat([df_BO_a_traiter,df_BO])
            df_paiement_restant = pd.concat([df_paiement_restant,df_paiement])
    return df_rapproche,df_paiement_restant,df_BO_a_traiter

def master_mapping_bo_paiement(df_paiement: pd.DataFrame,df_BO : pd.DataFrame,list_cols_clientname_payment: str=None,**kwargs):
    '''Cette fonction permet de rapprocher tous les cas de figure :
    + Rapprochement sur le nom de subscriber_name
    + Rapprochement sur le nom de co_subscriber_name
    + Rapprochement plusieurs personnes
    + Rapprochement avec Light check
    '''
    ### rapprochement sur la colonne subscriber_name
    df_rapproche = pd.DataFrame()
    df_rapproche,df_paiement,df_BO = mapping_paiement_bo(df_rapproche,df_paiement,df_BO,
                                                    list_cols_clientname_payment,mapping_type='basic',**kwargs)

    ### rapprochement sur la colonne cosubscriber_name
    df_BO = df_BO.rename(columns={'subscriber_name':'subscriber_name0',
                                'cosubscriber_name':'subscriber_name'})
    df_rapproche_cosub = pd.DataFrame()
    df_rapproche_cosub,df_paiement,df_BO = mapping_paiement_bo(df_rapproche_cosub,df_paiement,df_BO,
                                                    list_cols_clientname_payment,date_colname,mapping_type='basic',**kwargs)
    df_BO = df_BO.rename(columns={'subscriber_name':'cosubscriber_name',
                                'subscriber_name0':'subscriber_name'})
    df_rapproche_cosub = df_rapproche_cosub.rename(columns={'subscriber_name':'cosubscriber_name',
                                'subscriber_name0':'subscriber_name'})
    df_rapproche = pd.concat([df_rapproche,df_rapproche_cosub])

    ### rapprochement sur plusieurs personnes
    df_rapproche,df_paiement,df_BO = mapping_paiement_bo(df_rapproche,df_paiement,df_BO,
                                                    list_cols_clientname_payment,mapping_type='pls_pp',**kwargs)


    ### quelqu'un qui fait plusieurs paiements avec des comptes, et motifs différents
    df_rapproche,df_paiement,df_BO = mapping_paiement_bo(df_rapproche,df_paiement,df_BO,
                                                    list_cols_clientname_payment,mapping_type='pls_paiements_diff_motifs',**kwargs)


    df_rapproche,df_paiement,df_BO = mapping_paiement_bo(df_rapproche,df_paiement,df_BO,
                                                    list_cols_clientname_payment,mapping_type='light_check_paiementunique',**kwargs)


    df_rapproche,df_paiement,df_BO = mapping_paiement_bo(df_rapproche,df_paiement,df_BO,
                                                    list_cols_clientname_payment,mapping_type='light_check_pls_paiements_1ord',**kwargs)


    return df_rapproche,df_paiement,df_BO

def get_categorie(motif:str=None):
    return 'Light check' if 'light_check' in motif else 'Proposition'

def master_mapping_transfer_check(
    entity: str,
    df_paiement: pd.DataFrame,
    df_BO: pd.DataFrame,
    df_mapping_col: pd.DataFrame,
    col_paiements: List[str] = [],
    **kwargs
) -> pd.DataFrame:
    """
    This function is for matching transfers or checks for ABCD/xyz.
    Returns a DataFrame with the final results of matched and unmatched payments and BO orders.

    Parameters:
    - entity (str): Entity type, either 'ABCD' or 'xyz'.
    - df_paiement (pd.DataFrame): DataFrame containing client payment data (check or transfer table).
    - df_BO (pd.DataFrame): DataFrame containing BO orders/contracts not yet processed.
    - df_mapping_col (pd.DataFrame): DataFrame mapping column names between different entities.
    - col_paiements (List[str]): List of additional columns in the payment table.
    - kwargs: Additional optional parameters for matching:
        - list_cols_clientname_payment (List[str]): List of columns regarding the payer in the transfer/check table.
        - date_colname (str): Name of the Date column in the payment table.
        - amount_colname (str): Name of the Amount column in the payment table.
        - id_paiement (str): Name of the system ID column (unique ID) in the payment table.
        - amount_threshold (float): Acceptance threshold on the amount for matching.
        - min_score (int): Minimum accepted similarity score.
        - bo_name_col ([str]): Column name in the BO table for the subscriber name.
        - dict_nb_jours (Dict[str, int]): Dictionary containing the number of days before and after the subscription date to search for the payment.

    Returns:
    - pd.DataFrame: Final DataFrame with matched and unmatched payments and BO orders.
    """
    # Extract keyword arguments with defaults
    list_cols_clientname_payment = kwargs.get('list_cols_clientname_payment', [])
    date_colname = kwargs.get('date_colname')
    amount_colname = kwargs.get('amount_colname')
    id_paiement = kwargs.get('id_paiement')
    amount_threshold = kwargs.get('amount_threshold', 5)
    min_score = kwargs.get('min_score', 90)
    bo_name_col = kwargs.get('bo_name_col')
    dict_nb_jours = kwargs.get('dict_nb_jours', {})

    # Retain only necessary columns in the payment DataFrame and preserve column order
    col_paiements = [id_paiement, date_colname, amount_colname] + col_paiements + list_cols_clientname_payment
    df_paiement = df_paiement[col_paiements]
    col_BO = df_mapping_col[~df_mapping_col[entity].isnull()]['column'].tolist()
    df_BO = df_BO[col_BO]
    old_col = col_paiements + col_BO

    if entity == 'ABCD':
        df_BO_pp = df_BO[df_BO.share_type == 'Full ownership']
        df_BO_dm = df_BO[df_BO.share_type != 'Full ownership']

        # Process full ownership
        nb_days_period = dict_nb_jours['ABCD_PP']
        df_BO_pp['Start_Date'] = df_BO_pp['creation_date']
        df_BO_pp['End_Date'] = df_BO_pp['creation_date'] + dt.timedelta(days=nb_days_period)
        df_rapproche_pp, df_paiement, df_BO_pp = master_mapping_bo_paiement(df_paiement, df_BO_pp,
                                                                             list_cols_clientname_payment, **kwargs)

        # Process dismemberment
        nb_days_period = dict_nb_jours['ABCD_Démembrement']
        df_BO_dm['Start_Date'] = df_BO_dm['creation_date']
        df_BO_dm['End_Date'] = df_BO_dm['creation_date'] + dt.timedelta(days=nb_days_period)
        df_rapproche_dm, df_paiement, df_BO_dm = master_mapping_bo_paiement(df_paiement, df_BO_dm,
                                                                             list_cols_clientname_payment, **kwargs)
        df_rapproche = pd.concat([df_rapproche_pp, df_rapproche_dm])
        df_BO = pd.concat([df_BO_dm, df_BO_pp])
    else:
        # Process other entity
        nb_days_period = dict_nb_jours['xyz']
        df_BO['Start_Date'] = df_BO['creation_date']
        df_BO['End_Date'] = df_BO['creation_date'] + dt.timedelta(days=nb_days_period)
        df_rapproche, df_paiement, df_BO = master_mapping_bo_paiement(df_paiement, df_BO,
                                                                      list_cols_clientname_payment, **kwargs)

    # Display summary of results
    print(f'Number of matched payment rows: {len(df_rapproche)},\n' +
          f'Number of unmatched payment rows: {len(df_paiement)},\n' +
          f'Number of unmatched BO rows: {len(df_BO)}')

    # Map column names
    dict_name = dict(zip(df_mapping_col['column'], df_mapping_col[entity]))
    df_rapproche['categorie'] = df_rapproche['motif'].apply(get_categorie)
    df_paiement['categorie'] = 'Heavy check'  # Remaining rows to be manually matched

    # Concatenate and organize final DataFrame
    df_paiement_final = pd.concat([df_rapproche, df_paiement]).drop(columns=['Start_Date', 'End_Date', 'id_unique', 'nom_commun'], errors='ignore')
    new_col = [x for x in df_paiement_final.columns if x not in old_col]
    df_paiement_final = df_paiement_final[old_col + new_col].sort_values(by='categorie', ascending=False)
    df_paiement_final = df_paiement_final.rename(columns=dict_name)

    return df_paiement_final