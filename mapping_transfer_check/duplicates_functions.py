import datetime as dt
import pandas as pd
import sqlite3
from utils.utils import *

def agreger_par_interval_date(df_paiement: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Aggregates all transactions within a specified number of days for a person for each date.

    Parameters:
    - df_paiement: DataFrame containing payment information.
    - *args: Additional positional arguments.
    - **kwargs: Additional keyword arguments. Must include:
        - id_paiement: Unique identifier for the payment.
        - colonne_nomClient: Column name for the client's name.
        - colonne_date: Column name for the date.
        - colonne_montant: Column name for the amount.
        - nb_days: Number of days interval to aggregate (default 2).

    Returns:
    - DataFrame with aggregated payments.
    """
    id_paiement = kwargs.get('id_paiement')
    colonne_nomClient = kwargs.get('colonne_nomClient')
    colonne_date = kwargs.get('colonne_date')
    colonne_montant = kwargs.get('colonne_montant')
    nb_days = kwargs.get('nb_days', 2)
    
    # Etape 1: agréger les virements pour la même date et même personne pour éviter les doublons dans les étapes suivantes
    ### ie concaténer paiement1 et paiement2 => ça devient paiement12
    # 2.1 ajouter l'intervale 
    df_paiement = df_paiement[(df_paiement[colonne_nomClient]!='') & (~df_paiement[colonne_nomClient].isnull())]
    if len(df_paiement)>0:
        df_paiement_agrege = df_paiement.groupby(by=[colonne_nomClient,colonne_date]).\
                                                    agg({colonne_montant:'sum',\
                                                        id_paiement : "|".join   # concaténer les Id sys pour la même personne
                                                        }).reset_index()

        #Etape 2: chercher les virements dans l'interval de x jours (nb_days)
        # 2.1 ajouter l'intervale
        df1 = df_paiement_agrege[[id_paiement,colonne_nomClient,colonne_date,colonne_montant]].sort_values(by=[colonne_nomClient,colonne_date])
        df2 = df1.copy()
        id_paiement_2 = id_paiement +"_2"
        colonne_nomClient_2 = colonne_nomClient +"_2"
        colonne_date_2 = colonne_date +"_2"
        colonne_montant_2 = colonne_montant+"_2"
        df2.columns = [id_paiement_2,colonne_nomClient_2 ,colonne_date_2,colonne_montant_2]

        df1["End_date"] = df1[colonne_date]+dt.timedelta(days=+int(nb_days))

        # 2.2 flagger les virements dans l'intervale
        conn = sqlite3.connect(':memory:')
        df1.to_sql('df1', conn, index=False)
        df2.to_sql('df2', conn, index=False)
        qry = f' SELECT  * ' \
                'FROM df1 JOIN df2 ' \
                ' ON df1.' + colonne_nomClient  + ' = df2.' + colonne_nomClient_2 + \
                ' AND df2.'+ colonne_date_2 +' <= df1.End_date AND df2.'+colonne_date_2+' > df1.'+colonne_date


        df_match_date_sup = pd.read_sql_query(qry,conn)

        #2.3 supprimer les paiements déjà pris l'intervale du virement de la date de départ (ie enlever les doublons)
        ### normalement ça va créer 3 lignes : paiement12 et paiement3, paiement12 et paiement4, paiement3 et paiement4
        ### il faut donc suppprimer la ligne paiement3 et paiement4
        list_idsys2 = set(list(df_match_date_sup[id_paiement_2])) # list des Id sys qui sont déjà utilisés
        list_idsys_agreges = set(list(df_match_date_sup[id_paiement_2])+list(df_match_date_sup[id_paiement])) # tous les id sys
        df_match_date_sup = df_match_date_sup[~df_match_date_sup[id_paiement].isin(list_idsys2)] # enlever les lignes déjà utilisés
        list_idsys = set(list(df_match_date_sup[id_paiement])) #+list_concat_meme_date

        # 2.4 rajouter les virements de la date de départ
        ### ajouter la ligne paiement12 et paiement12 pour faire le groupby
        df_date0_to_add = df_paiement_agrege[df_paiement_agrege[id_paiement].isin(list_idsys)] 
        df_date0_to_add[id_paiement_2] = df_date0_to_add[id_paiement]
        df_date0_to_add[colonne_montant_2] = df_date0_to_add[colonne_montant]

        ### les lignes unitaires pour les gens qui n'ont pas fait pls virements ou les personnes qui ont fait 2 virement le même jour
        df_not_agrege = df_paiement_agrege[~df_paiement_agrege[id_paiement].isin(list_idsys_agreges)]
        df_not_agrege[id_paiement_2] = df_not_agrege[id_paiement]
        df_not_agrege[colonne_montant_2] = df_not_agrege[colonne_montant]

        # 2.5 : agréger les données
        ### df_match_date_sup :  paiement12 et paiement3, paiement12 et paiement4
        ### df_date0_to_add :  paiement12 et paiement12
        ### df_not_agrege : lignes unitaires
        df_match_date_sup = pd.concat([df_match_date_sup,df_date0_to_add,df_not_agrege]).sort_values([colonne_nomClient,colonne_date])
        df_match_date_sup[colonne_date] = pd.to_datetime(df_match_date_sup[colonne_date])

        # Prendre la date
        df_match_date_sup[colonne_date_2] = df_match_date_sup.apply(lambda f : get_date(f[id_paiement],
                                                                                f[id_paiement_2],
                                                                                f[colonne_date],
                                                                                f[colonne_date_2]), axis=1 )
        df_match_date_sup[colonne_date_2] = pd.to_datetime(df_match_date_sup[colonne_date_2],format = '%Y-%m-%d %H:%M:%S')
        df_match_date_sup = df_match_date_sup.groupby(by=[id_paiement,colonne_date]).agg({colonne_montant_2:"sum",
                                                                    colonne_nomClient:'first',
                                                                    id_paiement_2: "|".join,
                                                                    colonne_date_2:"max"
                                                                }).reset_index()
        df_match_date_sup[id_paiement] = df_match_date_sup[id_paiement].apply(lambda x: x.split("|")[0] if "|" in x else x)
        df_match_date_sup = df_match_date_sup[[id_paiement_2,colonne_date,colonne_nomClient,colonne_montant_2,colonne_date_2]]
        df_match_date_sup = df_match_date_sup.rename(columns={id_paiement_2:id_paiement,colonne_montant_2:colonne_montant,colonne_date_2:'Max_'+colonne_date})
        df_match_date_sup[colonne_montant+"_total"] = df_match_date_sup[colonne_montant]
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
        - colonne_montant: Amount column name (default 'amount').
        - colonne_date: Date column name (default 'effective_date').
        - colonne_nomClient: Client name column name (default 'clientname').
        - seuil_montant: Threshold amount for matching (default 5).

    Returns:
    - Merged DataFrame.
    '''
    columns_left = kwargs.get('columns_left')
    columns_right = kwargs.get('columns_right')
    id_left = kwargs.get('id_left')
    id_right = kwargs.get('id_right')
    colonne_montant = kwargs.get('colonne_montant', 'amount')
    colonne_date = kwargs.get('colonne_date', 'effective_date')
    colonne_nomClient = kwargs.get('colonne_nomClient', 'clientname')
    seuil_montant = kwargs.get('seuil_montant', 5)
    
    # Create combo name column
    df_duplicates.loc[:,'nompaiement_nombo'] = df_duplicates[colonne_nomClient] + '|' + df_duplicates.subscriber_name # créer la colonne combo des noms
    columns_left.append('nompaiement_nombo')
    columns_right.append('nompaiement_nombo')
    
    # Remove duplicates in each table
    df_left = df_duplicates[columns_left].drop_duplicates(subset=id_left) 
    df_right = df_duplicates[columns_right].drop_duplicates(subset=id_right)

    # Sort values
    df_left = df_left.sort_values(by=['nompaiement_nombo',colonne_date,colonne_montant])
    df_right = df_right.sort_values(by=['nompaiement_nombo','creation_date','total_amount'])
    
    # Create order by date
    df_left['index'] = df_left.groupby(["nompaiement_nombo",colonne_montant]).cumcount()+1 
    df_right['index'] = df_right.groupby(["nompaiement_nombo",'total_amount']).cumcount()+1
   
    # Merge by order of date
    df_merge = df_left.merge(df_right,on=['index','nompaiement_nombo'])
    df_merge['ecart_montant'] = abs(df_merge.total_amount - df_merge[colonne_montant])
    
    # Aggregate and filter by matching amount
    df_merge_agg = df_merge.groupby(by=['index',id_left])['ecart_montant'].min().reset_index()
    df_merge_agg = df_merge_agg[df_merge_agg.ecart_montant<=seuil_montant] 
    
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