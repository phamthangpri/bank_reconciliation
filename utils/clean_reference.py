import pandas as pd
import numpy as np
import re
import unidecode


df_motsprasites = pd.read_excel("./_config/word_list.xlsx",sheet_name="Sheet1",header=None)
pattern_clean_motif = "|".join(list(df_motsprasites[0])) ### Create Regex pattern to clean the motif



def clean_name(text: str = None) -> str:
    """
    Cleans the client name by removing terms indicating titles such as Mr., Mrs., Ms., etc.
    
    Parameters:
        text (str): The input text containing the client name.
    
    Returns:
        str: The cleaned client name.
    """
    
    if text is None or text == "NAN":
        return np.nan

    # Normalize text to uppercase and remove accents
    text = unidecode.unidecode(str(text).upper())
    
    # Define patterns for titles to be removed
    patterns = [
        # Monsieur or Madame
        r'\bM\.OU MME\s+', r'\bMRMME', r'\bM\.M\s+', r'\bM OU MME\s+', r'\bMOU ME\s+', r'\bMOU MME\s+',
        r'\bMR ET MME\s+', r'\bM\. et MME\s+', r'\bM\.OUMME\s+', r'\bM\+MME\s+', r'\bOUMR\s+', r'\bMME OU M\s+',
        # Monsieur
        r'\bM\s+', r'\s+M\b', r'\bDR\.\s+', r'\bDR\s+', r'\bM\.', r'\bM\s+', r'\bM\.\s+', r'\bMR\.\s+',
        r'\bMR\s+', r'\s+MR\b', r'\bMONSIEUR\s+', r'\bM\.OU\s+', r'\bSR\s+', r'\bSIR\s+', r'\bMONSIEUR',
        # Madame
        r'\bMME\s+', r'\s+MME\b', r'\bMADAME\s+', r'\bOU MME\s+', r'\bET MME\s+', r'\bE SRA\s+', r'\bMRS\s+',
        # Mademoiselle
        r'\bML\s+', r'\bMLLE\s+', r'\bMLE\s+', r'\bMELLE\s+', r'\bMISS\s+', r'\bMADEMOISELLE\s+',
        r'\bOU\s+', r'\bET\s+', r'\bET\.\s+'
    ]
    
    # Remove patterns from text
    for pattern in patterns:
        text = re.sub(pattern, '', text)
    
    # Extract only valid name characters (A-Z, spaces, digits, and apostrophes)
    text = ' '.join(re.findall(r'[A-Z\s\d\']+', text))
    
    # Remove extra spaces and trailing dots
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'\.', '', text).strip()
    
    # Clean redundant patterns if needed
    for _ in range(5):
        text = remove_de(text)
    
    return text

def clean_motif(text: str = None) -> str:
    """
    Cleans the motif by removing unnecessary words, numbers, special characters, and duplicate words.
    
    Parameters:
        text (str): The input text to clean.
    
    Returns:
        str: The cleaned text.
    """
    
    # List of patterns to replace
    list_replace = ["1/1", "1/LLE", "1/LME", "1/LONSIEUR", "1/LR"]
    
    # Convert text to string if not None
    text = str(text) if text is not None else ''
    
    # Return the original text if it's empty or NaN
    if text in {'', 'nan', 'NAN', np.nan, None}:
        return np.nan
    
    # Remove specified patterns
    for item in list_replace:
        text = text.replace(item, '')
    
    # Replace '/' with space
    text = re.sub(r'\/', ' ', text)
    
    # Clean and remove unwanted characters and words
    text = get_words_only(text)  # Assumes this function filters out unwanted characters
    
    if text:
        # Remove specific unwanted patterns
        text = re.sub(pattern_clean_motif, '', text)  # Assumes this pattern is defined elsewhere
        text = text.strip()
        
        # Remove single-character words and clean the name
        text = ' '.join(word for word in text.split() if len(word) > 1)
        text = clean_name(text)  # Assumes clean_name is defined elsewhere
        
        # Remove duplicate words
        text = remove_duplicated(text)  # Assumes remove_duplicated is defined elsewhere
        
        # Apply additional cleaning steps
        for _ in range(5):
            text = remove_de(text)  # Assumes remove_de is defined elsewhere

    return np.nan if text in {'nan', 'NAN', np.nan, '', None} else text
def get_words_only(text: str = None) -> str:
    """
    Extracts only alphabetic words from the input text, removing special characters and words containing digits.
    
    Parameters:
        text (str): The input text from which to extract words.
    
    Returns:
        str: A string containing only alphabetic words separated by spaces. Returns an empty string if no valid words are found.
    """
    
    if text:
        text = str(text)
        
        # Regular expression pattern to match words without special characters or digits
        pattern =  r'(\b[^\W\d]+\b)'
        
        # Find all matches using the regular expression pattern
        words = re.findall(pattern, text)
        
        # Join the list of words into a single string separated by spaces
        return ' '.join(words)

def remove_de(text: str = None) -> str:
    """
    Removes specific trailing words from the end of the input text if they appear at the end.
    Keeps these words if they appear at the beginning.
    
    Parameters:
        text (str): The input text from which to remove specific trailing words.
    
    Returns:
        str: The modified text with trailing words removed if they appear at the end. 
             Returns NaN if the text is one of the specified words to remove.
    """
    list_to_remove = ["DE", "DU", "ET","AU","LA", "LE",'OU']
    if text:
        text = str(text)
        
        # Remove 'ET ' or 'OU ' from the start of the text
        if text[:3] in ["ET ",'OU ']:
            return text[3:]
        
         # Remove specific words from the end of the text
        if text[-3:] in list_to_remove:
            return text[:-3]
        
        # If the text is reduced to one of the words to remove, return NaN
        if text in list_to_remove:
            return np.nan
    return text

def remove_duplicated(text: str = None) -> str:
    """
    Removes duplicate words in the input text, preserving the order of their first occurrence.

    Parameters:
        text (str): The input text from which to remove duplicate words.
    
    Returns:
        str: The text with duplicate words removed. If the input is NaN or empty, returns the input as is.
    """
    text = str(text)
    if text == np.nan or text == "" or text == "NAN": return text
    text_list = text.split()
    text_list = sorted(set(text_list), key = text_list.index)
    return " ".join(text_list)

def get_product_motif(motif1: str = None, motif2: str = np.nan) -> str:
    """
    Determines the product based on patterns in the provided motifs.

    Parameters:
        motif1 (str): The first motif to check for product patterns.
        motif2 (str): The second motif to check for product patterns (default is np.nan).
    
    Returns:
        str: The product identifier if a pattern match is found, otherwise np.nan.
    """
    motif1 = str(motif1)
    motif2 = str(motif2)
    
    # Define patterns for products
    patern_pd1 = r'PRODUIT1'
    patern_pd2 = r'\bPD2|COMPANYPD2'
    patern_pd3 = r'PRODUIT3\b|\PD3\b'
    
    # Check patterns in the given motifs
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