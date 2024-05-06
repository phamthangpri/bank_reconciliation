from bs4 import BeautifulSoup 
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz


def get_date(id1: str=None, id2: str=None, valeur1=None, valeur2=None):
    return valeur1 if id1==id2 else valeur2

def find_commun_word(text1: str=None,text2: str=None):
    '''Cette fonction permet de trouver un mot en commun entre 2 textes. Ce qui permet de comparer 2 textes et dire s'il y a un nom en commun entre les deux.
    '''
    if text1 and text2:
        return any(word in text2.split() and len(word)>2 and word != 'LES' for word in text1.split())
    return False


def clean_num_cheque(x: str=None):
    return str(x).replace('.0','').zfill(8)


def calculate_fuzz_score(df_data: pd.DataFrame, col1: str=None,col2: str=None,col_output:str ='max_score'):
    '''Cette fonction permet de calculer le score de similarité entre les deux textes (deux nom : nom sur le paiement vs nom dans le BO)
    '''
    list_null = [np.nan,'',None]
    mask = (~df_data[col1].isin(list_null)) & ~df_data[col2].isin(list_null)
    df_data.loc[mask,'token_sort_ratio'] = df_data.loc[mask,:].apply(lambda x : fuzz.token_sort_ratio(x[col1],x[col2]),axis=1)
    df_data.loc[mask,'full_score'] = df_data.loc[mask,:].apply(lambda x : fuzz.ratio(x[col1],x[col2]),axis=1)

    mask1 = mask & (df_data[col1].str.split().str.len()>1) & (df_data[col2].str.split().str.len()>1)
    ### pour les noms ayant plus d'un mot
    df_data.loc[mask1,'token_set_ratio'] = df_data.loc[mask1,:].apply(lambda x : fuzz.token_set_ratio(x[col1],x[col2]),axis=1)
    df_data.loc[mask1,'partial_ratio'] = df_data.loc[mask1,:].apply(lambda x : fuzz.partial_ratio(x[col1],x[col2]),axis=1)
    ### pour les noms ayant qu'un mot
    df_data.loc[mask,'token_set_ratio'] = df_data.loc[mask,'token_set_ratio'].fillna(0)
    df_data.loc[mask,'partial_ratio'] = df_data.loc[mask,'partial_ratio'].fillna(0)


    df_data[col_output] = df_data[['token_sort_ratio','full_score','token_set_ratio','partial_ratio']].max(axis=1)
    df_data = df_data.drop(columns=['token_sort_ratio','full_score','token_set_ratio','partial_ratio'])
    return df_data

def check_name(df_data: pd.DataFrame,list_names: list,score_threshold: int=90):
    '''Vérifier s'il y a un des noms dans la liste qui est similaire au nom dans le BO (le cas pour le virement, il y a à la fois le titulaire et le motif)
    '''
    list_cols = ['max_' + col for col in list_names]
    for col in list_names:
        df_data = calculate_fuzz_score(df_data, col,'subscriber_name',col_output='max_' + col)
    df_data['max_score'] = df_data[list_cols].max(axis=1)
    df_data = df_data[df_data.max_score>=score_threshold]
    df_data = df_data.drop(columns=list_cols)
    return df_data