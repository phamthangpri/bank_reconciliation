import datetime as dt
import pandas as pd
import numpy as np
from utils.utils import *
from mapping_transfer_check.basic_functions import *
from mapping_transfer_check.duplicates_functions import *

'''
L'idée générale : on va alimenter au fur et à mesure la dataframe df_rapproche pour les lignes déjà rapprochées
    Params:
        + df_paiement : dataframe sur le paiement du client (table de chèque ou virement)
        + df_BO : ordres/contrats de BO pas encore traités
        + colonnes_nomClient_paiement : liste des colonnes sur le payeur de la table de virement / chèque
        + colonne_date : nom de la colonne Date de la table de paiement
        + colonne_montant : nom de la colonne Montant de la table de paiement
        + id_paiement : nom de la colonne id sysètme (id unique) de la table de paiement
        + seuil_montant : seuil d'acceptation sur le montant pour le rapprochement
        + motif : la manière de rapprochement (paiement unique / npaiement_1ord,...) pour déclencher la fonction mais aussi pour flagger dans le résultat final
        + nb_jours_intervalle : Nombre jours maximum accepté entre la date de création des ordres et la date de paiement
        + min_score : le pourcentage minimum accepté pour le score de similarité
'''

def mapping_paiement_unique(
    df_rapproche: pd.DataFrame,
    df_paiement: pd.DataFrame,
    df_BO: pd.DataFrame,
    colonnes_nomClient_paiement: List[str],
    colonne_date: Optional[str] = None,
    colonne_montant: Optional[str] = None,
    id_paiement: Optional[str] = None,
    motif: Optional[str] = None,
    seuil_montant: int = 5,
    min_score: int = 90
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    '''Cette fonction permet de rapprocher un paiement avec un ordre de BO. La règle basique.
    '''
    for colonne_nomClient in colonnes_nomClient_paiement:
        df_match = rapprocher_paiement_bo_basic(
            df_paiement, df_BO, colonne_nomClient, colonne_date, colonne_montant, id_paiement,
            seuil_montant, min_score
        )
        df_match['motif'] = motif
        df_rapproche = pd.concat([df_rapproche, df_match], ignore_index=True)
        
        # Filter out matched records
        matched_payment_ids = set(df_match[id_paiement])
        matched_order_ids = set(df_match['order_id'])
        
        df_paiement = df_paiement[~df_paiement[id_paiement].isin(matched_payment_ids)]
        df_BO = df_BO[~df_BO['order_id'].isin(matched_order_ids)]
    return df_rapproche,df_paiement,df_BO

def mapping_npaiement_1ord(
    df_rapproche: pd.DataFrame,
    df_paiement: pd.DataFrame,
    df_BO: pd.DataFrame,
    colonnes_nomClient_paiement: List[str],
    colonne_date: Optional[str] = None,
    colonne_montant: Optional[str] = None,
    id_paiement: Optional[str] = None,
    motif: Optional[str] = None,
    nb_jours_intervalle: int = 2,
    seuil_montant: float = 5,
    min_score: int = 90,
    is_lightcheck: bool = False
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    '''Cette fonction permet de rapprocher plusieurs paiements pour un ordre.
    Pour ça, il va agréger les données de paiements sur une intervalle de date et utiliser la fonction mapping_paiement_unique (car les paiements agrégés = 1 paiement)
    '''
    nbdays_agreges = int(nb_jours_intervalle/2)
    for colonne_nomClient in colonnes_nomClient_paiement:
        colonne_date_2 =  'Max_'+ colonne_date
        colonne_montant_2 = colonne_montant +'_total'
        step = 2
        for nb_days in np.arange(4,nbdays_agreges+step,step=step): 
            ## Agréger les données de paiement par l'intervale de date
            df_paiement_agg = aggregate_by_date(df_paiement,id_paiement,colonne_nomClient,colonne_date,colonne_montant,nb_days)
            if len(df_paiement_agg)>0:
                # rapprocher avec le paiement agrégé
                if is_lightcheck :
                    df_match = create_light_check(df_paiement_agg,df_BO,colonne_nomClient,colonne_date_2,colonne_montant_2,id_paiement,seuil_montant)
                else: df_match = rapprocher_paiement_bo_basic(df_paiement_agg,df_BO,colonne_nomClient,colonne_date_2,colonne_montant_2,id_paiement,
                                                            seuil_montant,min_score)
                if len(df_match)>0:
                    list_col = list(df_BO.columns)
                    list_col.append(id_paiement)
                    df_match = df_match[list_col]

                    ## remettre le paiement unitaire
                    df_match[id_paiement] = df_match[id_paiement].str.split('|')
                    df_match = df_match.explode(id_paiement,ignore_index=True)
                    df_match = df_match.merge(df_paiement,on=id_paiement)
                    df_match['motif'] = motif
                    df_rapproche = pd.concat([df_rapproche,df_match])
                    df_paiement = df_paiement[~df_paiement[id_paiement].isin(df_rapproche[id_paiement])]
                    df_BO = df_BO[~df_BO.order_id.isin(df_rapproche.order_id)]
    return df_rapproche,df_paiement,df_BO

def mapping_1paiement_nord(
    df_rapproche: pd.DataFrame,
    df_paiement: pd.DataFrame,
    df_BO: pd.DataFrame,
    colonnes_nomClient_paiement: str=None,
    colonne_date: str=None,
    colonne_montant: str=None,
    id_paiement: str=None,
    motif: str=None,
    nb_jours_intervalle:int = 2,
    seuil_montant: float=5,
    min_score:int = 90):
    '''Cette fonction permet de rapprocher plusieurs ordres pour un paiement.
    Pour ça, il va agréger les données de BO sur une intervalle de date et utiliser la fonction mapping_paiement_unique (car les ordres agrégés = 1 ordre)
    ''' 
    step = 5
    nbdays_agreges = int(nb_jours_intervalle/2)
    for nb_days in np.arange(10,nbdays_agreges+step,step=step): 
        colonne_nomClient_bo = 'subscriber_name'
        id_paiement_bo = 'order_id'
        colonne_date_bo = 'creation_date'
        colonne_montant_bo = 'total_amount'
        ### agréger les ordres
        df_BO_agg = aggregate_by_date(df_BO,id_paiement_bo,colonne_nomClient_bo,colonne_date_bo,colonne_montant_bo,nb_days)
        if len(df_BO_agg) >0:
            df_BO_agg.loc[:,'Start_Date'] = df_BO_agg.creation_date
            df_BO_agg.loc[:,'End_Date'] = df_BO_agg.creation_date+dt.timedelta(days=+nb_jours_intervalle)


            for colonne_nomClient in colonnes_nomClient_paiement:
                ### rapprocher avec les données de BO agrégé
                df_match = rapprocher_paiement_bo_basic(df_paiement,df_BO_agg,colonne_nomClient,colonne_date,colonne_montant,id_paiement,
                                                        seuil_montant,min_score)
                df_BO_agg = df_BO_agg[~df_BO_agg.order_id.isin(df_match.order_id)]

                ### Remettre les ordres unitaires
                list_col = list(df_paiement.columns)
                list_col.append('order_id')
                df_match = df_match[list_col]
                df_match['order_id'] = df_match['order_id'].str.split('|')
                df_match = df_match.explode('order_id',ignore_index=True)
                df_match = df_match.merge(df_BO,on='order_id')
                df_match['motif'] = motif
                df_rapproche = pd.concat([df_rapproche,df_match])
                df_paiement = df_paiement[~df_paiement[id_paiement].isin(df_rapproche[id_paiement])]
                df_BO = df_BO[~df_BO.order_id.isin(df_rapproche.order_id)]
    return df_rapproche,df_paiement,df_BO

def mapping_plspersonnes(
    df_rapproche:pd.DataFrame, 
    df_paiement: pd.DataFrame, 
    df_BO: pd.DataFrame, 
    colonnes_nomClient_paiement: str=None,
    colonne_date: str=None, 
    colonne_montant: str=None, 
    id_paiement: str=None,
    motif: str=None, 
    nb_jours_intervalle:int = 2, 
    seuil_montant: float=5,
    min_score:int=90, 
    is_bo: bool=True):
    '''Chercher les contrats 2PP, concaténer les colonnes subscriber_name et cosubscriber_name 
    pour avoir 1 ord avec 2 lignes et 2 personnes différentes. 
    On va ensuite rapprocher ces lignes avec le paiement, et faire la somme des paiements pour comparer le montant
    Car normalement, chaque personne va payer une partie du montant d'ordre
    '''
    if is_bo: 
        df_BO_2pp = df_BO[(~df_BO.cosubscriber_name.isnull()) & ~df_BO.subscriber_name.isnull()]
        df_BO_2PP_1 = df_BO_2pp[['order_id','product_code','total_amount','creation_date','subscriber_name']]
        df_BO_2PP_2 = df_BO_2pp[['order_id','product_code','total_amount','creation_date','cosubscriber_name']].rename(columns={
                                                                                                'cosubscriber_name':'subscriber_name'
                                                                                            })
        df_BO_2pp = pd.concat([df_BO_2PP_1,df_BO_2PP_2])

        df_BO_2pp.loc[:,'Start_Date'] = df_BO_2pp.creation_date
        df_BO_2pp.loc[:,'End_Date'] = df_BO_2pp.creation_date+dt.timedelta(days=+nb_jours_intervalle)
        ### rapprocher avec date et produit uniquement
        df_match = mapping_approximately(df_paiement,df_BO_2pp,colonne_montant,colonne_date,seuil_montant=-1000)
    else: df_match = mapping_approximately(df_paiement,df_BO,colonne_montant,colonne_date,seuil_montant=-1000)
    df_match = df_match[~df_match.order_id.isnull()]
    df_match = df_match[~df_match[id_paiement].isnull()]
    if len(df_match) > 0:
        ### trouver la bonne personne avec le score
        df_match = check_name(df_match,colonnes_nomClient_paiement,min_score)
        df_match = df_match[['order_id','total_amount',colonne_montant,id_paiement]].drop_duplicates()
        
        ### comparer le montant
        df_match_agg = df_match.groupby(by=['order_id','total_amount']).agg({colonne_montant:'sum',
                                                                            id_paiement:'|'.join}).reset_index()
        df_match_agg['ecart_montant'] = abs(df_match_agg.total_amount-df_match_agg[colonne_montant])
        df_match_agg = df_match_agg[df_match_agg.ecart_montant<=seuil_montant]

        ### re-merger avec les tables pour avoir toutes les infos
        df_match_agg[id_paiement] = df_match_agg[id_paiement].str.split('|')
        df_match_agg = df_match_agg.explode(id_paiement,ignore_index=True)

        df_match_agg = df_match_agg[['order_id',id_paiement]]
        df_match = df_match_agg.merge(df_paiement,on=id_paiement)
        df_match = df_match.merge(df_BO,on='order_id')
        df_match['motif'] = motif
        df_rapproche = pd.concat([df_rapproche,df_match])
        df_BO = df_BO[~df_BO.order_id.isin(df_match.order_id)]
        df_paiement = df_paiement[~df_paiement[id_paiement].isin(df_match[id_paiement])]
    return df_rapproche, df_paiement,df_BO

def mapping_lightcheck_paiementunique(
    df_rapproche: pd.DataFrame, 
    df_paiement: pd.DataFrame, 
    df_BO: pd.DataFrame, 
    colonnes_nomClient_paiement: str=None, 
    colonne_date: str=None, 
    colonne_montant: str=None,
    id_paiement: str=None, 
    motif:str = None, 
    seuil_montant: float=5, 
    colonne_nom_bo: str='subscriber_name'):
    '''Cette fonction permet de rapprocher avec une règle moins stricte : on cherche s'il y a le nom du client en commumn
    entre le paiement et le BO. 
    '''
    # df_rapproche_approx = pd.DataFrame()
    for colonne_nomClient in colonnes_nomClient_paiement:
        df_match = create_light_check(df_paiement,df_BO,colonne_nomClient,colonne_date,
                                        colonne_montant,id_paiement,seuil_montant,colonne_nom_bo)
        df_match['motif'] = motif
        df_rapproche = pd.concat([df_rapproche,df_match])
        df_paiement = df_paiement[~df_paiement[id_paiement].isin(df_rapproche[id_paiement])]
        df_BO = df_BO[~df_BO.order_id.isin(df_rapproche.order_id)]
    return df_rapproche,df_paiement,df_BO



