import pandas as pd
from pathlib import Path
from sentence_transformers import SentenceTransformer, util
import os
import re

def regex_match_string(ngram_list, regex_list, mystring):
    if any(regex.search(mystring) for regex in regex_list):
        return 1
    elif any(regex in mystring for regex in ngram_list):
        return 1
    else:
        return 0

if __name__ == '__main__':
    ngrams_dict = {
        'MX': {
            'lost_job_1mo': ['me despidieron', 'perd[i|í] mi trabajo',
                             'me corrieron', 'me qued[e|é] sin (trabajo|chamba|empleo)',
                             'ya no tengo (trabajo|empleo|chamba)'],
            'is_hired_1mo': ['consegui[\w\s\d]*empleo', 'nuevo trabajo',
                             'nueva chamba', 'encontr[e|é][.\w\s\d]*trabajo',
                             'empiezo[\w\s\d]*trabajar', 'primer d[i|í]a de trabajo'],
            'is_unemployed': ['estoy desempleado', 'sin empleo', 'sin chamba', 'nini',
                              'no tengo (trabajo|empleo|chamba)',
                              ],
            'job_search': ['necesito[\w\s\d]*trabajo', 'busco[\w\s\d]*trabajo', 'buscando[\w\s\d]*trabajo',
                           'alguien[\w\s\d]*trabajo', 'necesito[\w\s\d]*empleo'],
            'job_offer': ['empleo', 'contratando', 'empleo nuevo', 'vacante', 'estamos contratando']},
        'BR': {
            'lost_job_1mo': ['perdi[.\w\s\d]*emprego', 'perdi[.\w\s\d]*trampo',
                             'fui demitido', 'me demitiram',
                             'me mandaram embora'],
            'is_hired_1mo': ['consegui[.\w\s\d]*emprego', 'fui contratad[o|a]',
                             'começo[.\w\s\d]*emprego', 'novo emprego|emprego novo',
                             'primeiro dia de trabalho', 'novo (emprego|trampo)'],
            'is_unemployed': ['estou desempregad[a|o]', 'eu[.\w\s\d]*sem[.\w\s\d]*emprego',
                              ],
            'job_search': ['gostaria[.\w\s\d]*emprego', 'queria[.\w\s\d]*emprego', 'preciso[.\w\s\d]*emprego',
                           'procurando[.\w\s\d]*emprego'],
            'job_offer': ['enviar[.\w\s\d]*curr[í|i]culo', 'envie[.\w\s\d]*curr[í|i]culo',
                          'oportunidade[.\w\s\d]*emprego',
                          'temos[.\w\s\d]*vagas']},
    }
