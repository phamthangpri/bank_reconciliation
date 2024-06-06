import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz


def get_date(id1: str = None, id2: str = None, valeur1: str = None, valeur2: str = None) -> str:
    """
    Returns the value of `valeur1` if `id1` is equal to `id2`; otherwise, returns the value of `valeur2`.

    Parameters:
        id1 (str): The first identifier to compare.
        id2 (str): The second identifier to compare.
        valeur1 (str): The value to return if `id1` equals `id2`.
        valeur2 (str): The value to return if `id1` does not equal `id2`.

    Returns:
        str: `valeur1` if `id1` equals `id2`, otherwise `valeur2`.
    """
    return valeur1 if id1 == id2 else valeur2

def find_commun_word(text1: str = None, text2: str = None) -> bool:
    """
    This function checks for any common word between two text strings, ensuring the common word 
    is longer than 2 characters and is not 'LES'. It helps in comparing two texts to find a common name.

    Parameters:
        text1 (str: The first text string.
        text2 (str): The second text string.

    Returns:
        bool: True if there is a common word longer than 2 characters (excluding 'LES'), False otherwise.
    """
    if text1 and text2:
        # Split both texts into words and check for any common word meeting the criteria
        return any(word in text2.split() and len(word) > 2 and word != 'LES' for word in text1.split())
    return False


def clean_num_cheque(x: str = None) -> str:
    """
    Cleans and formats a cheque number by removing trailing '.0' and padding with leading zeros.

    Parameters:
        x (str): The cheque number as a string. Can be a number represented as a string or a float.

    Returns:
        str: The cleaned and zero-padded cheque number as an 8-character string.
    """
    # Convert the input to a string, remove any trailing '.0', and pad with leading zeros to ensure an 8-character length.
    return str(x).replace('.0', '').zfill(8)


def calculate_fuzz_score(df_data: pd.DataFrame, col1: str, col2: str, col_output: str = 'max_score') -> pd.DataFrame:
    """
    Calculates similarity scores between text in two columns of a DataFrame using fuzzy matching techniques.

    This function computes various fuzzy matching scores to compare text in `col1` with text in `col2`:
    - `token_sort_ratio`: Compares sorted tokens of both texts.
    - `full_score`: Compares entire texts.
    - `token_set_ratio`: Compares token sets, accounting for token overlaps.
    - `partial_ratio`: Compares partial matches.

    Parameters:
        df_data (pd.DataFrame): DataFrame containing the text data.
        col1 (str): Name of the first column containing the text for comparison.
        col2 (str): Name of the second column containing the text for comparison.
        col_output (str): Name of the column where the maximum similarity score will be stored. Defaults to 'max_score'.

    Returns:
        pd.DataFrame: DataFrame with an additional column for the maximum similarity score between `col1` and `col2`.
    """
    
    # List of values considered as null or empty
    list_null = [np.nan, '', None]
    
    # Create a mask to filter out rows where either column has null or empty values
    mask = (~df_data[col1].isin(list_null)) & ~df_data[col2].isin(list_null)
    
    # Calculate various fuzzy matching scores
    df_data.loc[mask, 'token_sort_ratio'] = df_data.loc[mask].apply(lambda x: fuzz.token_sort_ratio(x[col1], x[col2]), axis=1)
    df_data.loc[mask, 'full_score'] = df_data.loc[mask].apply(lambda x: fuzz.ratio(x[col1], x[col2]), axis=1)
    
    # Calculate scores for texts with more than one word
    mask1 = mask & (df_data[col1].str.split().str.len() > 1) & (df_data[col2].str.split().str.len() > 1)
    df_data.loc[mask1, 'token_set_ratio'] = df_data.loc[mask1].apply(lambda x: fuzz.token_set_ratio(x[col1], x[col2]), axis=1)
    df_data.loc[mask1, 'partial_ratio'] = df_data.loc[mask1].apply(lambda x: fuzz.partial_ratio(x[col1], x[col2]), axis=1)
    
    # Fill NaN values with 0 for single-word texts
    df_data.loc[mask, 'token_set_ratio'] = df_data.loc[mask, 'token_set_ratio'].fillna(0)
    df_data.loc[mask, 'partial_ratio'] = df_data.loc[mask, 'partial_ratio'].fillna(0)
    
    # Determine the maximum score from the calculated scores
    df_data[col_output] = df_data[['token_sort_ratio', 'full_score', 'token_set_ratio', 'partial_ratio']].max(axis=1)
    
    # Drop intermediate columns used for calculations
    df_data = df_data.drop(columns=['token_sort_ratio', 'full_score', 'token_set_ratio', 'partial_ratio'])
    
    return df_data

def check_name(df_data: pd.DataFrame, list_names: list, score_threshold: int = 90) -> pd.DataFrame:
    """
    Checks if any name in a list is similar to the name in the 'subscriber_name' column of the DataFrame
    using fuzzy matching scores. 

    The function calculates fuzzy matching scores between each name in the `list_names` and the 'subscriber_name' column,
    then filters the DataFrame to include only rows where the highest matching score is above the specified threshold.

    Parameters:
        df_data (pd.DataFrame): DataFrame containing the data with 'subscriber_name' column.
        list_names (list): List of column names in `df_data` to compare against 'subscriber_name'.
        score_threshold (int): Minimum score threshold for considering a match. Default is 90.

    Returns:
        pd.DataFrame: DataFrame filtered to include only rows where the highest fuzzy matching score is above the threshold.
    """
    
    # Generate column names for storing the maximum fuzzy scores
    list_cols = ['max_' + col for col in list_names]
    
    # Calculate fuzzy matching scores for each name column in list_names
    for col in list_names:
        df_data = calculate_fuzz_score(df_data, col, 'subscriber_name', col_output='max_' + col)
    
    # Determine the highest score across all columns and filter based on the score threshold
    df_data['max_score'] = df_data[list_cols].max(axis=1)
    df_data = df_data[df_data['max_score'] >= score_threshold]
    
    # Drop the intermediate columns used for fuzzy score calculations
    df_data = df_data.drop(columns=list_cols)
    
    return df_data