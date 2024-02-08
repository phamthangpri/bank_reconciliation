import pandas as pd
import numpy as np
import re
import unidecode


df_motsprasites = pd.read_excel("./_config/word_list.xlsx",sheet_name="Sheet1",header=None)
pattern_clean_motif = "|".join(list(df_motsprasites[0])) ### Create Regex pattern to clean the motif



def clean_name(text: str=None):
    '''Supprimer les mots qui indiquent Madame, Monsieur, Mademoiselle dans le nom de client
    '''
    list_to_replace_name = [# Monsieur ou Madame 
                                 r'(\bM\.OU MME\s+)',r'(\bMRMME)',r'(\bM\.M\s+)',r'(\bM OU MME\s+)',r'(\bMOU ME\s+)',r'(\bMOU MME\s+)', \
                                 r'(\bMR ET MME\s+)',r'(\bM\. et MME\s+)', r'(\bM\.OUMME\s+)', r'(\bM\+MME\s+)',r'(\bOUMR\s+)', r'(\bMME OU M\s+)',\
                                 # Monsieur
                                r'(\bM\s+)',r'(\s+M\b)',r'(\bDR.\s+)',r'(\bDR\s+)', r'(\bM\.)',r'(\bM\s+)',r'(\bM.\s+)', r'(\bMR\.\s+)',\
                                r'(\bMR\s+)',r'(\s+MR\b)',r'(\bMONSIEUR\s+)', r'(\bM\.OU\s+)', r'(\bSR\s+)',r'(\bSIR\s+)',r'(\bMONSIEUR)',\
                                # Madame
                                 r'(\bMME\s+)',r'(\s+MME\b)', r'(\bMADAME\s+)', r'(\bOU MME\s+)', r'(\bET MME\s+)',r'(\bE SRA\s+)',r'(\bMRS\s+)',\
                                # Mademoiselle
                                 r'(\bML\s+)', r'(\bMLLE\s+)',r'(\bMLE\s+)', r'(\bMELLE\s+)',r'(\bMISS\s+)', r'(\bMADEMOISELLE\s+)' \
                                 r'(\bOU\s+)',r'(\bET\s+)',r'(\bET\.\s+)'
                                 ]  
    text = unidecode.unidecode(str(text).upper())
    if text == np.nan or text == "NAN": return np.nan
    for item in list_to_replace_name:
        text = re.sub(item, "",text)
    pattern_clean_name = r'([A-Z\s\d\']*)'
    text = ' '.join(re.findall(pattern_clean_name,text))
    text = re.sub(r' +', " ",text)
    text = re.sub(r'\.', "",text).strip()
    for i in range(0,5): 
        text = remove_de(text)
    return text
def clean_motif(text: str=None):
    '''Nettoyer le motif en supprimant : les mots parasites, les chiffres, les caractères spéciaux et les mots en doublon
    '''
    list_replace = ["1\/1","1\/LLE","1\/LME","1\/LONSIEUR","1\/LR"]
    text = str(text)
    if text == np.nan or text == "": return text
    for item in list_replace:
        text = text.replace(item,"")
    text = re.sub(r'\/'," ",text)
    text = get_words_only(text)
    if text != '' and text !=np.nan and not text == None:
        text = re.sub(pattern_clean_motif,"",text) ### pattern_clean_motif = liste des mots parasites qui est créée tout en haut
        text = text.strip()
        text = ' '.join(w for w in text.split(" ") if len(w) > 1)
        text = clean_name(text)
        text = remove_duplicated(text)
        for i in range(0,5): 
            text = remove_de(text)
    if text in ['nan','NAN',np.nan,'',None]: return np.nan
    return text

def get_words_only(text: str=None):
    '''Extraire uniquement les mots (sans caractères spéciaux, ni des mots avec des chiffres) afin de récupérer le nom, prénom
    '''
    if text:
        text = str(text)
        patern_text = r'(\b[^\W\d]+\b)'
        list_result = re.findall(patern_text, text)
        if len(list_result)>0:
            return " ".join(list_result)

def remove_de(text: str=None):
    '''A la fin des motifs nettoyés, il y a souvent des mots DE (qui vient de Souscription de COMPANY,...etc). Cette fonction va supprimer uniquement les DE à la fin du nom
    Les DE au début sont gardés (pour les noms de famille DE quelque choses)
    '''
    list_to_remove = ["DE", "DU", "ET","AU","LA", "LE",'OU']
    if text:
        text = str(text)
        if text[:3] in ["ET ",'OU ']:
            return text[3:]
        if text[-3:] in list_to_remove:
            return text[:-3]
        if text in list_to_remove:
            return np.nan
    return text

def remove_duplicated(text: str=None):
    ''''Pour supprimer les noms et prénoms en doublons dans les motifs. Si un mot apparait plusieurs fois, ce serait supprimé pour le garder une fois
    '''
    text = str(text)
    if text == np.nan or text == "" or text == "NAN": return text
    text_list = text.split()
    text_list = sorted(set(text_list), key = text_list.index)
    return " ".join(text_list)

def get_product_motif(motif1: str=None,motif2: str=np.nan):
    motif1 = str(motif1)
    motif2 = str(motif2)
    
    patern_pd1 = r'PRODUIT1'
    patern_pd2 = r'\bAA|COMPANYAA'
    patern_pd3 = r'ORIGIN\b|\bCC\b'
    if re.search(patern_pd2,motif1):
        result = "PD2"
    elif re.search(patern_pd2,motif2):
        result = "PD2"
    elif re.search(patern_pd1,motif1):
        result = "PD1"
    elif re.search(patern_pd1,motif2):
        result = "PD1"
    elif re.search(patern_pd3,motif1):
        result = "PD3"
    elif re.search(patern_pd3,motif2):
        result = "PD3"
    else : result = np.nan
    return result