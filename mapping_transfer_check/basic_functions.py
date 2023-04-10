import pandas as pd
import sqlite3
from utils.utils import *
from mapping_transfer_check.duplicates_functions import *

## Ces fonctions appliquent le rapprochement sur une colonne de payeur donné VS les données BO

def mapping_approximately(
    df_payment: pd.DataFrame,
    df_BO: pd.DataFrame,
    amount_colname: Optional[str] = None,
    date_colname: Optional[str] = None,
    amount_threshold: float = 5.0
) -> pd.DataFrame:
    with sqlite3.connect(':memory:') as conn:
        df_payment.to_sql('releve', conn, index=False)
        df_BO.to_sql('BO', conn, index=False)
        
        base_query = f"""
            SELECT *
            FROM releve
            LEFT JOIN BO ON (
                releve.{date_colname} BETWEEN BO.Start_Date AND BO.End_Date
        """
        
        if amount_threshold != -1000:
            query = f"""
                {base_query}
                AND ABS(releve.{amount_colname} - BO.total_amount) <= ?
            )
            """
            df_match = pd.read_sql_query(query, conn, params=[amount_threshold])
        else:
            query = f"""
                {base_query}
                AND releve.{amount_colname} <= BO.total_amount
            )
            """
            df_match = pd.read_sql_query(query, conn)
    
    return df_match



def rapprocher_paiement_bo_basic(df_payment: pd.DataFrame,df_rebo_ordre_vir: pd.DataFrame,
    colonne_nomClient: str=None,date_colname: str=None,amount_colname: str=None,payment_id: str=None,
    amount_threshold: float=5, min_score:int = 90):
    '''C'est le rapprochement basic, 1 paiement = 1 ord
    Les colonnes en entrée sont de la table df_payment, car celles du BO ne changent pas.
    Le seuil d'acceptation pour la montant et l'éccart de date sont à modifiés en entrée. Le score de similarité pour les nom est >= 90%
    token_sort_score : il va trier les mots par ordre alphabetique avant de comparer
    token_set_ratio : trier et supprimer les doublons
    full_score : prendre tous les mots en tant que tels
    partial_score : vérifier si un mot contient l'autre
    '''
    #Etape 1 : code SQL pour prendre les transactions qui ont le même montant et les dates proches
    df_match = mapping_approximately(df_payment,df_rebo_ordre_vir,amount_colname,date_colname,amount_threshold)

    #Etape 2 : calculer le score fuzzy sur le résultat obtenu. 
    df_match = df_match[~df_match[colonne_nomClient].isnull() & ~df_match['subscriber_name'].isnull()]
    df_match = df_match[df_match[colonne_nomClient].str.len()>=4]
    if len(df_match) > 0: 
        
        df_match = calculate_fuzz_score(df_match,colonne_nomClient,'subscriber_name')
        min_score = 90
        df_match = df_match[df_match["max_score"]>=min_score].reset_index(names='id_unique')
    
        ### Etape 3 : Traiter les doublons
        ### 3.1 séparer les doubons
        mask1 = df_match.duplicated(subset=payment_id,keep=False)  # doublons sur les paiements
        df_duplicates1 = df_match[mask1]
        mask2 = df_match.duplicated(subset='order_id',keep=False) # doublons sur les ordres
        df_duplicates2 = df_match[mask2]
        df_duplicates = pd.concat([df_duplicates1,df_duplicates2]).drop_duplicates(subset='id_unique') # concat les deux types de doublon
        df_ok = df_match[~df_match.id_unique.isin(df_duplicates.id_unique)]
        ### 3.2 Merger les doublons
        columns_left = list(df_payment.columns)
        columns_right = list(df_rebo_ordre_vir.columns)
        id_left = payment_id
        id_right = 'order_id'
        df_merge = merge_duplicates_by_date(df_duplicates,columns_left,columns_right,id_left,id_right,amount_colname,date_colname,colonne_nomClient)
        
        ### 3.3 Concaténer le résultat
        if len(df_merge)>0 : df_match = pd.concat([df_merge,df_ok])
        else: df_match = df_ok
        df_match = df_match.drop(columns=['id_unique', 'max_score'])
    return df_match

def create_light_check(df_payment: pd.DataFrame, df_BO: pd.DataFrame, colonne_nomClient: str=None,
    date_colname: str=None, amount_colname: str=None, payment_id: str=None, amount_threshold: float=5, 
    colonne_nom_bo: str='subscriber_name'):
    ''' Perform a light check by matching payments and BO orders based on date and amount,
    and then refine the matches based on common words in client names.
    
    Parameters:
        df_payment (pd.DataFrame): DataFrame containing payment data.
        df_BO (pd.DataFrame): DataFrame containing BO order data.
        colonne_nomClient (Optional[str]): Column name for client name in df_payment.
        date_colname (Optional[str]): Column name for date in df_payment.
        amount_colname (Optional[str]): Column name for amount in df_payment.
        payment_id (Optional[str]): Column name for payment ID in df_payment.
        amount_threshold (float): Threshold amount for matching. Default is 5.
        colonne_nom_bo (str): Column name for subscriber name in df_BO. Default is 'subscriber_name'.
    
    Returns:
        pd.DataFrame: DataFrame containing the matched records.
    '''
    
    ### rapprocher par date et montant, sans prendre en compte le nom
    df_match = mapping_approximately(df_payment,df_BO,amount_colname,date_colname,amount_threshold)
    ### Chercher le nom en commun
    mask = (~df_match.order_id.isnull()) & (~df_match[colonne_nomClient].isnull())
    df_match = df_match.loc[mask,:]
    if not df_match.empty:
        df_match['nom_commun'] = df_match.apply(lambda f : find_commun_word(f[colonne_nomClient],f[colonne_nom_bo]),axis=1)
        df_match = df_match[df_match.nom_commun==True].reset_index(names='id_unique')

        ####### traiter les doublons:
        df_duplicates1 = df_match[df_match.duplicated(subset=payment_id,keep=False)]
        df_duplicates2 = df_match[df_match.duplicated(subset='order_id',keep=False)]
        df_duplicates = pd.concat([df_duplicates1,df_duplicates2]).drop_duplicates(subset='id_unique')
        
        df_ok = df_match[~df_match.id_unique.isin(df_duplicates.id_unique)]
        df_duplicates_matched = pd.DataFrame()
        for i in range(0,5): ### il y a max 5 virements en doublon
            df_merge = merge_duplicates_by_date(
                df_duplicates,
                list(df_payment.columns),
                list(df_BO.columns),
                payment_id,
                'order_id',
                amount_colname,
                date_colname,
                colonne_nomClient
            )
            df_duplicates_matched = pd.concat([df_duplicates_matched,df_merge])
            df_duplicates = df_duplicates[~df_duplicates.order_id.isin(df_duplicates_matched.order_id)]
            df_duplicates = df_duplicates[~df_duplicates[payment_id].isin(df_duplicates_matched[payment_id])]
        df_match = pd.concat([df_duplicates_matched,df_ok])
    else : df_match = pd.DataFrame()
    return df_match

