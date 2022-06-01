"""
author: https://github.com/jonas-nothnagel and https://github.com/alinacherkas
"""

import pandas as pd
import numpy as np
import string  
import nltk 
import spacy
import en_core_web_sm
import wordninja

# standard library
import re, json, hashlib
from typing import Set, Tuple, List, Dict, Iterable, Callable, Union

# utils
from langdetect import detect

'''basic cleaning - suitable for transformer models'''
def basic(s):
    """
    :param s: string to be processed
    :return: processed string: see comments in the source code for more info
    """
    # Text Lowercase
    s = s.lower() 
    # Remove punctuation
    s = re.sub(r'\\n', ' ', s) 
    s = re.sub(r'\\r', ' ', s) 
    s = re.sub(r'\\xa0', ' ', s) 
    s = re.sub(r'\n', ' ', s) 
    translator = str.maketrans(' ', ' ', string.punctuation) 
    s = s.translate(translator)
    # Remove URLs
    s = re.sub(r'^https?:\/\/.*[\\r\\n]*', ' ', s, flags=re.MULTILINE)
    s = re.sub(r"http\S+", " ", s)
    # Remove new line characters

    # Remove distracting single quotes
    s = re.sub(r"\'", " ", s) 
    # Remove all remaining numbers and non alphanumeric characters
    s = re.sub(r'\d+', ' ', s) 
    s = re.sub(r'\W+', ' ', s)


    # define custom words to replace: 
    s = re.sub('content','',s,1)
    s = re.sub('checks background','',s,1)
    #s = re.sub(r'strengthenedstakeholder', 'strengthened stakeholder', s)
    
    return s.strip()

'''processing with spacy - suitable for models such as tf-idf, word2vec'''
def spacy_clean(alpha:str, use_nlp:bool = True) -> str:

    """

    Clean and tokenise a string using Spacy. Keeps only alphabetic characters, removes stopwords and

    filters out all but proper nouns, nounts, verbs and adjectives.

    Parameters
    ----------
    alpha : str

            The input string.

    use_nlp : bool, default False

            Indicates whether Spacy needs to use NLP. Enable this when using this function on its own.

            Should be set to False if used inside nlp.pipeline   

     Returns
    -------
    ' '.join(beta) : a concatenated list of lemmatised tokens, i.e. a processed string

    Notes
    -----
    Fails if alpha is an NA value. Performance decreases as len(alpha) gets large.
    Use together with nlp.pipeline for batch processing.

    """

    nlp = spacy.load("en_core_web_sm", disable=["parser", "ner", "textcat"])

    if use_nlp:

        alpha = nlp(alpha)

        

    beta = []

    for tok in alpha:

        if all([tok.is_alpha, not tok.is_stop, tok.pos_ in ['PROPN', 'NOUN', 'VERB', 'ADJ']]):

            beta.append(tok.lemma_)

            
    text = ' '.join(beta)
    text = text.lower()
    return text

"""
This module provides utilities for further cleaning UNDP jobs data.
"""
def remove_enum_url(string):
    # Remove urls
    result_string = re.sub(r'(http|https)\s*:\s*/\s*/[\w\\-]+(\.[\w\\-]+)+\s*\S*|www\.\S*', '', string)
    # Remove roman enum
    result_string = re.sub(r'•|\s[\(]*[i]+[\)]*[\.]*\s|\s[\(]*[i]*v[i]*[\)]*[\.]*\s', '', result_string) 
    result_string = re.sub(r'\(i+\)|\([i]*v[i]*\)', '', result_string)
    result_string = re.sub(r'\(\*\)', '', result_string)
    # Remove enum characters or digit
    result_string = re.sub(r'(?:\s|\;|\.|:)(\(*[a-z|\d]\))|(?:\s|\;|\.|:)([a-z|\d]\.)', '', result_string) 
    return result_string 

def clean_list(string_list):
    # Remove special characters
    result_list = [x.replace(u'\xa0', u' ')\
            .replace('\n',' ')\
            .replace('\r',' ')\
            .replace(':',' : ')\
            .strip()\
            for x in string_list if x!='' and not x.startswith('refer a friend')]
    # Remove Urls and enumeration characters
    result_list = [remove_enum_url(s) for s in result_list]
    return result_list

def detect_language(text: str) -> str:
    """
    Detects text source language.
    """
    language = detect(text)

    return language

def extract_language(string):
    try:
        return detect_language(string)
    except:
        return np.nan
    
def split_joint_words(string_field):
    words = string_field.split(' ')

    splitted_text = []
    for word in words:
        split = wordninja.split(word)
        if (len(split) > 1) and not any([s for s in word if s in string.punctuation]):
            splitted_text += split
        else:
            splitted_text.append(word)
    return ' '.join(splitted_text)

def clean_location(text: str, mapping: Dict[str, str]) -> str:
    """
    Standardises location by extracting county and truning it into an ISO code.
    """
    if not isinstance(text, str):
        location = 'Unspecified'

    elif re.search('remote|home|virtual', text):
        location = 'Home-based'

    else:
        for k, v in mapping.items():
            if k in text:
                location = v
                break

        else:
            location = 'Unspecified'

    return location

def get_region(text: str, mapping: Dict[str, str]) -> str:
    """
    Converts country ISO codes into one of UNDP's regions.
    """
    if text in ['Unspecified', 'Home-based']:
        region = text

    else:
        region = mapping[text]

    return region

def clean_contracts(text: str) -> str:
    """
    Standardises contract types.
    """
    contracts = {'individual contract': 'Individual Contract',
                 'service contract': 'Service Contract',
                 'fta local': 'FTA Local',
                 'fta international': 'FTA International',
                 'ald international': 'ALD International',
                 'ald local': 'ALD Local',
                 'ta international': 'TA International',
                 'ta local': 'TA local',
                 '100 series': '100 Series',
                 '200 series': '200 Series',
                 'internship': 'Internship',
                 'unv': 'UNV',
                 'ipsa (regular)': 'IPSA',
                 "npsa (regular)": "NPSA" ,                                                                          
                "npsa (short-term)": "NPSA short-term",                                                                             
                "ipsa (short-term)" : "IPSA short-term",
                 'other': 'Other'}

    if not isinstance(text, str):
        contract = 'Unspecified'

    else:
        for k, v in contracts.items():
            if k in text:
                contract = v
                break

        else:
            contract = 'Unspecified'

    return contract

def clean_posts(text: str) -> str:
    """
    Standardises post names and steps.
    """
    if not isinstance(text, str):
        post = 'Unspecified'

    elif re.search('(international|national) consultant', text):
        post = re.search('(international|national) consultant', text).group(0).title()

    elif re.search('(sb|sc|gs|g)(-| | - )?[1-9]{1,2}', text):
        step = re.search('\d+', text).group(0)
        post = f'SB/SC/GS-{step}'

    elif re.search('no(-)?[a-d]', text):
        step = re.search('[a-d]{1}', text).group(0).capitalize()
        post = f'NO-{step}'

    elif re.search('p(-)?[1-5]{1}', text):
        step = re.search('\d+', text).group(0)
        post = f'P-{step}'

    elif re.search('d(-)?[1-2]{1}', text):
        step = re.search('\d+', text).group(0)
        post = f'D-{step}'

    elif re.search('intern', text):
        post = re.search('intern', text).group(0).capitalize()

    elif re.search('unv', text):
        post = re.search('unv', text).group(0).upper()

    elif re.search('ipsa', text):
        post = re.search('ipsa', text).group(0).upper()

    elif re.search('npsa', text):
        post = re.search('npsa', text).group(0).upper()

    elif re.search('other|usg|asg|ald|ssa|ics|l-', text):
        post = 'Other'

    else:
        post = 'Other'

    return post

def clean_languages(text: str) -> List[str]:
    """
    Standardises required languages by extracting any required UN-official languages.
    """
    lgs = ['english', 'french', 'spanish', 'arabic', 'russian', 'chinese']

    if not isinstance(text, str):
        languages = 'Unspecified'

    elif re.search('|'.join(lgs), text):
        languages = ', '.join([x.capitalize() for x in lgs if x in text])

    else:
        languages = 'Unspecified'

    return languages

def generate_key(text: str, n: int = 32, prefix: str = '') -> str:
    """
    Generates a hash-based key of length n for any given string.

    Parameters
    ----------
    alpha: str
        input string to be hashed.
    n: int, defaults to 32
        desired length of the output key which must be an even number.
    prefix: str, defaults to an empty string
        prefix to use before the key.

    Returns
    -------
    result: str
        hash value of length n.
    """
    key = prefix + hashlib.shake_128(text.encode()).hexdigest(n // 2)
    return key

def split_joint_words(string_field):
    words = string_field.split(' ')

    splitted_text = []
    for word in words:
        split = wordninja.split(word)
        if (len(split) > 1) and not any([s for s in word if s in string.punctuation]):
            splitted_text += split
        else:
            splitted_text.append(word)
    return ' '.join(splitted_text)