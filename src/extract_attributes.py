import itertools
import numpy as np
import re

def get_meta_attributes(string_list):
    return ''.join(list(itertools.takewhile(lambda el: el != 'background', string_list)))

def get_background(string_list):
    idx = string_list.index('background')
    return ''.join(list(itertools.takewhile(lambda el: el != 'duties and responsibilities',string_list[idx:])))

def get_duties_and_responsibilities(string_list):
    idx = string_list.index('duties and responsibilities')
    return ''.join(list(itertools.takewhile(lambda el: el != 'competencies', string_list[idx:])))

def get_competencies(string_list):
    idx = string_list.index('competencies')
    return ''.join(list(itertools.takewhile(lambda el: el != 'required skills and experience', string_list[idx:])))

def get_required_skills(string_list):
    idx = string_list.index('required skills and experience')
    return ''.join(string_list[idx+1:])

def extract_meta_attribute(attribute, string_list):
    for l in string_list:
        if re.findall(attribute + '[\s]+:',l):
            return(l.split(':')[1].strip())   
    return np.nan

def has_attribute(string_list, attribute):
    if attribute in string_list:
        return 1 
    return 0