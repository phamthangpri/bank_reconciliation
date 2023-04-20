import pandas as pd
import datetime as dt
from mapping_transfer_check.basic_functions import *
from mapping_transfer_check.different_types_mapping_functions import *

'''Params :
        + entity : SCPI / Life
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

def mapping_paiement_bo(df_rapproche: pd.DataFrame,df_paiement_a_traiter: pd.DataFrame,df_BO_a_traiter: pd.DataFrame,
                        list_cols_clientname_payment:str,date_colname:str,amount_colname:str,id_paiement:str,
                        nb_days_period:int,amount_threshold:float=5,min_score:int = 90,bo_name_col:str=None,mapping_type:str=None):
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
                                                                            list_cols_clientname_payment,date_colname,\
                                                                            amount_colname,id_paiement,\
                                                                            motif,amount_threshold,min_score)

                    ### 2. Plusieurs paiements pour un ordre :
                    motif = 'pls_paiement_1ord'
                    df_rapproche,df_paiement,df_BO = mapping_npaiement_1ord(df_rapproche,df_paiement,df_BO,\
                                                                            list_cols_clientname_payment,date_colname,\
                                                                            amount_colname,id_paiement,\
                                                                            motif,nb_days_period,amount_threshold,min_score)     

                    ### 3. Un paiement pour plusieurs ordres

                    motif = '1paiement_pls_ord'
                    df_rapproche,df_paiement,df_BO = mapping_1paiement_nord(df_rapproche,df_paiement,df_BO,\
                                                                            list_cols_clientname_payment,date_colname,\
                                                                            amount_colname,id_paiement,\
                                                                            motif,nb_days_period,amount_threshold,min_score)    
                    mask = df_rapproche.mauvais_compte.isnull()
                    df_rapproche.loc[mask,'mauvais_compte'] = mauvais_compte
                elif mapping_type == 'pls_pp':
                    motif = 'souscription_pls_pp'
                    df_rapproche,df_paiement,df_BO = mapping_npeople(df_rapproche,df_paiement,df_BO,
                                                        list_cols_clientname_payment,date_colname,
                                                        amount_colname,id_paiement,
                                                        motif,nb_days_period,amount_threshold,min_score,is_bo=True)
                    mask = df_rapproche.mauvais_compte.isnull()
                    df_rapproche.loc[mask,'mauvais_compte'] = mauvais_compte
                elif mapping_type == 'pls_paiements_diff_motifs':
                    motif = 'pls_paiement_differents_motifs'
                    df_match,df_paiement,df_BO = mapping_npeople(df_rapproche,df_paiement,df_BO,
                                                    list_cols_clientname_payment,
                                                    date_colname,amount_colname,id_paiement,
                                                    motif,nb_days_period,amount_threshold,min_score,is_bo=False)
                    mask = df_rapproche.mauvais_compte.isnull()
                    df_rapproche.loc[mask,'mauvais_compte'] = mauvais_compte
                elif mapping_type == 'light_check_paiementunique':
                    motif = mapping_type
                    df_rapproche, df_paiement,df_BO = mapping_lightcheck_uniquepayment(df_rapproche,df_paiement,df_BO,
                                                        list_cols_clientname_payment,
                                                        date_colname,amount_colname,
                                                        id_paiement,motif, amount_threshold,bo_name_col)
                    mask = df_rapproche.mauvais_compte.isnull()
                    df_rapproche.loc[mask,'mauvais_compte'] = mauvais_compte
                elif mapping_type == 'light_check_pls_paiements_1ord':
                    motif = mapping_type
                    df_rapproche,df_paiement,df_BO = mapping_npaiement_1ord(df_rapproche,df_paiement,df_BO,\
                                                        list_cols_clientname_payment,date_colname,\
                                                        amount_colname,id_paiement,\
                                                        motif,nb_days_period,amount_threshold,min_score,is_lightcheck = True) 
                    mask = df_rapproche.mauvais_compte.isnull()
                    df_rapproche.loc[mask,'mauvais_compte'] = mauvais_compte
                df_BO_a_traiter = pd.concat([df_BO_a_traiter,df_BO])
            df_paiement_restant = pd.concat([df_paiement_restant,df_paiement])
    return df_rapproche,df_paiement_restant,df_BO_a_traiter

def master_mapping_bo_paiement(df_paiement: pd.DataFrame,df_BO : pd.DataFrame,list_cols_clientname_payment: str=None,
    date_colname: str=None, amount_colname: str=None, id_paiement: str=None, nb_days_period: int=2,
    amount_threshold: float=5,min_score:int = 90, bo_name_col: str='subscriber_name'):
    '''Cette fonction permet de rapprocher tous les cas de figure :
    + Rapprochement sur le nom de subscriber_name
    + Rapprochement sur le nom de co_subscriber_name
    + Rapprochement plusieurs personnes
    + Rapprochement avec Light check
    '''
    ### rapprochement sur la colonne subscriber_name
    df_rapproche = pd.DataFrame()
    df_rapproche,df_paiement,df_BO = mapping_paiement_bo(df_rapproche,df_paiement,df_BO,
                                                    list_cols_clientname_payment,date_colname,
                                                    amount_colname,id_paiement,
                                                    nb_days_period,amount_threshold,min_score,bo_name_col,mapping_type='basic')

    ### rapprochement sur la colonne cosubscriber_name
    df_BO = df_BO.rename(columns={'subscriber_name':'subscriber_name0',
                                'cosubscriber_name':'subscriber_name'})
    df_rapproche_cosub = pd.DataFrame()
    df_rapproche_cosub,df_paiement,df_BO = mapping_paiement_bo(df_rapproche_cosub,df_paiement,df_BO,
                                                    list_cols_clientname_payment,date_colname,
                                                    amount_colname,id_paiement,
                                                    nb_days_period,amount_threshold,min_score,bo_name_col,mapping_type='basic')
    df_BO = df_BO.rename(columns={'subscriber_name':'cosubscriber_name',
                                'subscriber_name0':'subscriber_name'})
    df_rapproche_cosub = df_rapproche_cosub.rename(columns={'subscriber_name':'cosubscriber_name',
                                'subscriber_name0':'subscriber_name'})
    df_rapproche = pd.concat([df_rapproche,df_rapproche_cosub])

    ### rapprochement sur plusieurs personnes
    df_rapproche,df_paiement,df_BO = mapping_paiement_bo(df_rapproche,df_paiement,df_BO,
                                                    list_cols_clientname_payment,date_colname,
                                                    amount_colname,id_paiement,
                                                    nb_days_period,amount_threshold,min_score,bo_name_col,mapping_type='pls_pp')


    ### quelqu'un qui fait plusieurs paiements avec des comptes, et motifs différents
    df_rapproche,df_paiement,df_BO = mapping_paiement_bo(df_rapproche,df_paiement,df_BO,
                                                    list_cols_clientname_payment,date_colname,
                                                    amount_colname,id_paiement,
                                                    nb_days_period,amount_threshold,min_score,bo_name_col,mapping_type='pls_paiements_diff_motifs')


    df_rapproche,df_paiement,df_BO = mapping_paiement_bo(df_rapproche,df_paiement,df_BO,
                                                    list_cols_clientname_payment,date_colname,
                                                    amount_colname,id_paiement,
                                                    nb_days_period,amount_threshold,min_score,bo_name_col,mapping_type='light_check_paiementunique')


    df_rapproche,df_paiement,df_BO = mapping_paiement_bo(df_rapproche,df_paiement,df_BO,
                                                    list_cols_clientname_payment,date_colname,
                                                    amount_colname,id_paiement,
                                                    nb_days_period,amount_threshold,min_score,bo_name_col,mapping_type='light_check_pls_paiements_1ord')


    return df_rapproche,df_paiement,df_BO

def get_categorie(motif:str=None):
    return 'Light check' if 'light_check' in motif else 'Proposition'

def master_mapping_transfer_check(entity: str,df_paiement: pd.DataFrame,df_BO: pd.DataFrame,df_mapping_col:pd.DataFrame, 
                                 col_paiements : list=[], list_cols_clientname_payment: str=None,\
                                 date_colname: str=None,amount_colname: str=None,id_paiement: str=None,\
                                 amount_threshold: float=5,min_score:int = 90, bo_name_col: str=None, dict_nb_jours:dict={}):
    '''Cette fonction est pour rapprocher les virements ou chèques pour SCPI/Life.
    Return : tuple (table_ordres_rapprochés, table_paiement_non_rapproché, table_BO_non_rapproché)
    '''
    ## enlever les colonnes inutles et garder l'ordre des colonnes en entrée
    col_paiements = [id_paiement, date_colname,amount_colname] + col_paiements + list_cols_clientname_payment
    df_paiement = df_paiement[col_paiements]
    col_BO = df_mapping_col[~df_mapping_col[entity].isnull()]['column'].to_list()
    df_BO = df_BO[col_BO]
    old_col = col_paiements + col_BO

    if entity == 'SCPI':
        df_BO_pp = df_BO[df_BO.share_type == 'Full ownership']
        df_BO_dm = df_BO[df_BO.share_type != 'Full ownership']
        
        nb_days_period = dict_nb_jours['SCPI_PP'] ### le nombre de jours avant et après le date de souscription pour chercher le paiement
        df_BO_pp.loc[:,'Start_Date'] = df_BO_pp.creation_date
        df_BO_pp.loc[:,'End_Date'] = df_BO_pp.creation_date+dt.timedelta(days=+nb_days_period)

        df_rapproche_pp,df_paiement,df_BO_pp = master_mapping_bo_paiement(df_paiement,df_BO_pp,
                                                                    list_cols_clientname_payment,date_colname,
                                                                    amount_colname,id_paiement,
                                                                    nb_days_period,amount_threshold,min_score,bo_name_col)


        nb_days_period = dict_nb_jours['SCPI_Démembrement'] ### le nombre de jours avant et après le date de souscription pour chercher le paiement
        df_BO_dm.loc[:,'Start_Date'] = df_BO_dm.creation_date
        df_BO_dm.loc[:,'End_Date'] = df_BO_dm.creation_date+dt.timedelta(days=+nb_days_period)

        df_rapproche_dm,df_paiement,df_BO_dm = master_mapping_bo_paiement(df_paiement,df_BO_dm,
                                                                    list_cols_clientname_payment,date_colname,
                                                                    amount_colname,id_paiement,
                                                                    nb_days_period,amount_threshold,min_score,bo_name_col)
        df_rapproche = pd.concat([df_rapproche_pp,df_rapproche_dm])
        df_BO = pd.concat([df_BO_dm,df_BO_pp])
    else : 
        nb_days_period = dict_nb_jours['LIFE'] ### le nombre de jours avant et après le date de souscription pour chercher le paiement
        df_BO.loc[:,'Start_Date'] = df_BO.creation_date
        df_BO.loc[:,'End_Date'] = df_BO.creation_date+dt.timedelta(days=+nb_days_period)
        df_rapproche,df_paiement,df_BO = master_mapping_bo_paiement(df_paiement,df_BO,
                                                                    list_cols_clientname_payment,date_colname,
                                                                    amount_colname,id_paiement,
                                                                    nb_days_period,amount_threshold,min_score,bo_name_col)
    print(f'Nb lignes de paiement rapprochées : {len(df_rapproche)},\n'+
            f'Nb lignes de paiement non rapprochées : {len(df_paiement)},\n'+
            f'Nb lignes de BO non rapprochées: {len(df_BO)}')
    
    dict_name = dict(zip(df_mapping_col['column'],df_mapping_col[entity]))
    df_rapproche['categorie'] = df_rapproche['motif'].apply(get_categorie)
    df_paiement['categorie'] = 'Heavy check' ### les lignes restant à rapprocher manuellement
    df_paiement_final = pd.concat([df_rapproche,df_paiement]).drop(columns=['Start_Date','End_Date','id_unique','nom_commun'],errors='ignore')
    ## réorganiser les colonnes
    new_col = [x for x in df_paiement_final.columns if x not in old_col]
    df_paiement_final = df_paiement_final[old_col + new_col].sort_values(by='categorie',ascending=False)
    df_paiement_final = df_paiement_final.rename(columns=dict_name)
    # df_BO = df_BO.rename(columns=dict_name)
    return df_paiement_final