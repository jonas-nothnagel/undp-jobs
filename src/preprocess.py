import glob, os, sys; sys.path.append('../src')
from datetime import datetime

from langdetect import detect
from tabulate import tabulate 
# data wrangling
import numpy as np
import pandas as pd
import re
import ast
import itertools
import wordninja
from dateutil import parser
import wordninja
import string
import matplotlib.pyplot as plt

from tqdm.auto import tqdm
tqdm.pandas()

'''import helper functions'''
import clean as clean
import extract_attributes as ex

'''multiprocessing'''
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)


# let's load the data
df = pd.read_csv("../data/undp_jobs.csv") 
original_size = df.shape[0]
print("length of raw dataset:", original_size)

# apply basic cleaning
df['cleaned_content'] = df['content'].apply(clean.basic)

# Binary column whether or not a posting follows the structure (background, competencies, sills and experiences, ...)
template_structure = ['background',
                      'duties and responsibilities',
                      'competencies',
                      'required skills and experience']
for section in template_structure:
    df['has_' + section.replace(' ','_')] = df['cleaned_content'].apply(lambda l: ex.has_attribute(l, section))

# Remove job postings not following the given structure and corresponding binary columns
df = df[(df['has_background']!=0)&\
           (df['has_duties_and_responsibilities']!=0)&\
           (df['has_competencies']!=0)&\
           (df['has_required_skills_and_experience']!=0)]

df.drop(['has_background',\
                  'has_duties_and_responsibilities',\
                  'has_competencies',\
                  'has_required_skills_and_experience'], axis=1, inplace=True)

print('Rows following job posting template structure: ', df.shape[0])
print('Number of projects that are not following the structure and are thus dropped:', original_size- df.shape[0])

# Extract string list of meta attributes such as application deadline, job title, ...
df['meta_atributes'] = df['cleaned_content'].apply(lambda l: ex.get_meta_attributes(l))

# Extract job posting components following given template
df['background'] = df['cleaned_content'].parallel_apply(lambda l: ex.get_background(l))
df['duties_and_responsibilities'] = df['cleaned_content'].parallel_apply(lambda l: ex.get_duties_and_responsibilities(l))
df['competencies'] = df['cleaned_content'].parallel_apply(lambda l: ex.get_competencies(l))
df['required_skills_and_experience'] = df['cleaned_content'].parallel_apply(lambda l: ex.get_required_skills(l))

# Extract meta data
meta_attributes = ['location', 'type of contract', 'starting date',
                   'application deadline', 'post level', 'duration of initial contract',
                   'languages required', 'expected duration of assignment']

# Apply to DataFrame
df['title'] = df['meta_atributes'].apply(lambda l: ex.extract_title(l, meta_attributes[0]))

for m_attr in meta_attributes:
    df[m_attr.replace(' ', '_')] = df['meta_atributes'].apply(
        lambda l: ex.extract_meta_attribute(m_attr, l, meta_attributes)
    )


