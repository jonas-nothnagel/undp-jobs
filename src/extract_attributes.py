import itertools
import numpy as np
import re

def get_meta_attributes(string_list):
    return ''.join(list(itertools.takewhile(lambda el: el != 'background', string_list)))


def get_background(input_string, start_phrase="background", end_phrase="duties and responsibilities"):
    pattern = f"{re.escape(start_phrase)}(.*?){re.escape(end_phrase)}"
    match = re.search(pattern, input_string, re.IGNORECASE | re.DOTALL)
    
    if match:
        return match.group(1).strip()
    else:
        return f"The phrase '{start_phrase}' or '{end_phrase}' was not found in the input string."
    
def get_duties_and_responsibilities(input_string, start_phrase="duties and responsibilities", end_phrase="competencies"):
    pattern = f"{re.escape(start_phrase)}(.*?){re.escape(end_phrase)}"
    match = re.search(pattern, input_string, re.IGNORECASE | re.DOTALL)
    
    if match:
        return match.group(1).strip()
    else:
        return f"The phrase '{start_phrase}' or '{end_phrase}' was not found in the input string."
    
def get_competencies(input_string, start_phrase="competencies", end_phrase="required skills and experience"):
    pattern = f"{re.escape(start_phrase)}(.*?){re.escape(end_phrase)}"
    match = re.search(pattern, input_string, re.IGNORECASE | re.DOTALL)
    
    if match:
        return match.group(1).strip()
    else:
        return f"The phrase '{start_phrase}' or '{end_phrase}' was not found in the input string."
    
def get_required_skills(input_string, start_phrase="required skills and experience"):
    pattern = f"{re.escape(start_phrase)}(.*?)$"
    match = re.search(pattern, input_string, re.IGNORECASE | re.DOTALL)
    
    if match:
        return match.group(1).strip()
    else:
        return f"The phrase '{start_phrase}' was not found in the input string."


    
def has_attribute(input_string, attribute):
    pattern = f"\\b{re.escape(attribute)}\\b"
    if re.search(pattern, input_string, re.IGNORECASE):
        return 1
    return 0

def remove_full_duplicates(string):
    # Split the string into words
    words = string.split()
    
    # Find the length of the potential repeated sequence
    for i in range(1, len(words) // 2 + 1):
        if words[:i] * (len(words) // i) == words[:len(words) - (len(words) % i)]:
            return ' '.join(words[:i])
    
    # If no repetition found, return the original string
    return string

def extract_meta_attribute(attribute, input_string, meta_attributes):
    try:
        current_index = meta_attributes.index(attribute)
    except ValueError:
        return None

    start_phrase = attribute

    if current_index < len(meta_attributes) - 1:
        end_phrase = meta_attributes[current_index + 1]
        pattern = f"{re.escape(start_phrase)}(.*?){re.escape(end_phrase)}"
    else:
        end_phrase = "background"
        pattern = f"{re.escape(start_phrase)}(.*?){re.escape(end_phrase)}"

    match = re.search(pattern, input_string, re.IGNORECASE | re.DOTALL)

    if match:
        extracted_text = match.group(1).strip()
        # Remove full duplicates
        return remove_full_duplicates(extracted_text)
    else:
        if current_index == len(meta_attributes) - 1:
            pattern = f"{re.escape(start_phrase)}(.*?)$"
            match = re.search(pattern, input_string, re.IGNORECASE | re.DOTALL)
            if match:
                extracted_text = match.group(1).strip()
                # Remove full duplicates
                return remove_full_duplicates(extracted_text)
        
        return None

def extract_title(input_string, first_attribute):
    pattern = f"(.*?){re.escape(first_attribute)}"
    match = re.search(pattern, input_string, re.IGNORECASE | re.DOTALL)
    if match:
        extracted_text = match.group(1).strip()
        # Remove full duplicates
        return remove_full_duplicates(extracted_text)
    return None

def has_attribute(string_list, attribute):
    if attribute in string_list:
        return 1 
    return 0