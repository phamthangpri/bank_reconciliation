from bs4 import BeautifulSoup 
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz


def read_xml(path_to_xml: str=None, folder: str=None, filename: str=None, encoding: str='utf-8', bol_pei: bool=False):
    '''Read direct debit xml files.
    '''
    with open(path_to_xml + folder + filename, 'r', encoding=encoding) as f:
        xml_file = f.read()
    bs_data = BeautifulSoup(xml_file, 'xml') # scrapper les données

    #### Récupérer les données selon class ###
    id_remise = bs_data.findAll("PmtInfId")
    credit_time = bs_data.findAll('CreDtTm')
    nb_transactions = bs_data.findAll("NbOfTxs")
    montant_total = bs_data.findAll("CtrlSum")
    client_names = bs_data.findAll("Nm")
    ord_names = bs_data.findAll("MndtId")
    date_signs = bs_data.findAll("DtOfSgntr")
    montants = bs_data.findAll("InstdAmt")
    products = bs_data.findAll("EndToEndId")
    
    

    ### Nettoyer données récupérées ###
    id_remise = id_remise[0].get_text()
    credit_time = credit_time[0].get_text()
    nb_transactions = nb_transactions[0].get_text()
    montant_total = montant_total[0].get_text()
    if bol_pei == True:
        ord_prlv = products
    else:    ord_prlv = products[0].get_text().split()[0]
    

    data = []
    for i in range(0,len(ord_names)):
        rows = [client_names[i+2].get_text(),ord_names[i].get_text(), ## les 2 premiers names dans client_names est CORUM AM à exclure
               date_signs[i].get_text(),montants[i].get_text(),products[i].get_text()[0:2]]
        data.append(rows)

    df_xml_file = pd.DataFrame(data,columns = ['client_name','order_name','date_sign','amount','product'])
    df_xml_file["session_id"] = id_remise
    df_xml_file["credit_date"] = credit_time
    df_xml_file["nb_transactions"] = nb_transactions
    df_xml_file["total_amount"] = montant_total
    df_xml_file["filename"] = filename
    df_xml_file["ord_prlv"] = ord_prlv

    ### Pour les sessions de PEI, le product est n'est trouvé que sur le filename
    if "SES_Epargne" in filename:
        df_xml_file["product"] = df_xml_file["filename"].apply(lambda x: x.split("_")[2])
    ### Formatting ### 
    df_xml_file["amount"] = df_xml_file["amount"].astype(float)
    df_xml_file["total_amount"] = df_xml_file["total_amount"].astype(float)
    df_xml_file["nb_transactions"] = df_xml_file["nb_transactions"].astype(int)
    df_xml_file.credit_date = pd.to_datetime(df_xml_file.credit_date.str[0:10],format='%Y-%m-%d')
    df_xml_file["date_sign"] = pd.to_datetime(df_xml_file["date_sign"],format='%Y-%m-%d')
    return df_xml_file


def get_date(id1: str=None, id2: str=None, valeur1=None, valeur2=None):
    return valeur1 if id1==id2 else valeur2

def find_commun_word(text1: str=None,text2: str=None):
    '''Cette fonction permet de trouver un mot en commun entre 2 textes. Ce qui permet de comparer 2 textes et dire s'il y a un nom en commun entre les deux.
    Par ex : 
    text1 = 'O HEUSSNER P HEUSSNER J HEUSSNER', text2 = 'ODETTE HEUSSNER'
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