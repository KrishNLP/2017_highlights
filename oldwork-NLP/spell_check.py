#!/usr/bin/python
# -*- coding: utf-8 -*-

from nltk.tokenize.punkt import PunktSentenceTokenizer, PunktParameters
import enchant
import re
from nltk.tokenize import word_tokenize
import string
from nltk.corpus import stopwords
from itertools import groupby
import pandas as pd
import os
from pycorenlp import StanfordCoreNLP
from fuzzywuzzy import fuzz

import simplejson as json
from orderedset import OrderedSet
from collections import Counter


#testing functions
# from norvig_correction_funct import correction as norvig_correction
# from norvig_correction_funct import candidates as norvig_candidates
# from norvig_correction_funct import P as norvig_probability
# from norvig_correction_funct import enablePrint
# from emoji import pattern as regex_compiled_emoji

#prodct functions

from .norvig_correction_funct import correction as norvig_correction
from .norvig_correction_funct import candidates as norvig_candidates
from .norvig_correction_funct import candidates as norvig_candidates
from .norvig_correction_funct import P as norvig_probability
from .norvig_correction_funct import enablePrint
from .emoji import pattern as regex_compiled_emoji

enablePrint()

stop = list(set(stopwords.words('english')))
ENCHANT_VERIFY = enchant.Dict('en_US')

script_path = 'resources/text_files/'


def check_abbrv_pattern(stry):
    if stry[-1] == '.':
         return stry[:-1]
    else:
         return stry

with open(script_path +'common_abbrv.txt', 'r') as abr, \
    open(script_path + 'text_abbrv', 'r') as SMS_coll,  \
    open(script_path + 'wide_distance_corrections.txt', 'r') as wide_distance, \
    open(script_path + 'textspeak_non_acronym', 'r') as priority_tokens, \
    open(script_path + 'contractions.json', 'r') as contractions, \
    open(script_path + 'single_message_file.txt', 'r') as whatsapp:


    main_dict = {
        'Abbreviations_Dictionary':
                    {
                        'pipeline_pos' : 2,
                        'read_in' : {check_abbrv_pattern(x.split(',')[0]):x.split(',')[1].strip('\n') for x in abr.readlines()}
                    },
        'SMS_Colloqualisms':
                    {
                        'pipline_pos' : 3,
                        'read_in' : {ln.split(':')[0].lower(): ln.split(':')[1].strip('\n').strip(' ').lower() for ln in SMS_coll.readlines()}
                    },
        'Wide_Distance_corrections' :

                    {
                        'pipline_pos' : 5,
                        'read_in' : {line.split(':')[0]: line.split(':')[1] for line in wide_distance.readlines()}
                    },
        'Priority_tokens' :
                    {
                        'pipeline_pos' : 1,
                        'read_in' : {word.split(',')[0].strip():word.split(',')[1].strip('\n').strip() for word in priority_tokens.readlines()}
                    },

        'Contractions' :
                    {
                        'pipeline_pos' : 4,
                        'read_in' : json.load(contractions)
                    },
        'whatsapp_messages' : #updated_regularly
                    {
                        'pipline_pos' : 4,
                        'read_in' : [msg.strip('\n') for msg in whatsapp.readlines()]
                    }
        }

def process_whatsapp(l_messages):
    is_common_word = []
    is_common_acronym = []
    is_non_upper_acronym = []
    for item in l_messages:
        if re.search(r'^\w+', item):
            item = re.sub(r"IMG-\d{8}-WA\d{0,5}\.jpg \(file attached\)|(?<= )[!\.\?](?= )", '' ,item)
            strip_emoji = wide_build_emoji(item)
            if strip_emoji:
                for tok in word_tokenize(strip_emoji):
                    if ENCHANT_VERIFY.check(tok) == True and tok not in stop and len(tok) >= 4:#
                        if re.search(r'\d+', tok): #exclude digits
                            pass
                        else:
                            is_common_word.append(str(tok))
                    elif re.search(r'(?<=\b)[A-Z]{2,}', tok) and len(set(list(tok))) > 1:
                        #sFIND common known entities
                        punc_split = re.split(r'\W', tok)
                        for ii in punc_split:
                            if ii.isupper() and len(ii) <= 7 and ENCHANT_VERIFY.check(ii.lower()) is False:
                                is_common_acronym.append(ii.lower())
                for non_upper_tok in word_tokenize(strip_emoji):
                    if not non_upper_tok.isupper() and non_upper_tok.upper() in list(map(str.upper, is_common_acronym)):
                        is_non_upper_acronym.append(non_upper_tok.lower())
    return (Counter(is_non_upper_acronym + is_common_acronym), Counter(is_common_word))


punkt_param = PunktParameters()
punkt_param.abbrev_types = set(list(main_dict['Abbreviations_Dictionary']['read_in'].keys()))


def abbreviation_case_(text_blob, standard_search, source_file):
    list_of_matched_objects = []
    def find_replace_value(matchobj):
        matched = matchobj.group(0)
        if matched + '.' in text_blob:
            compile_new_pattern = re.compile(r'(?=\b)(?<=\b)%s\.' % re.escape(matched))
            if re.search(compile_new_pattern, text_blob):
                try:
                    list_of_matched_objects.append(source_file[matched])
                except KeyError:
                    pass
        return source_file[matched]
    all_expanded = re.sub(standard_search, find_replace_value, text_blob)
    add_to_abbreviations = r'(?<=\b)([A-z]\.){1,}[A-z](?=\b)'
    # add_to_abbreviations = r'(?<=\b )([A-z]\.){1,}[A-z](?=\b)|(?<=\b)\w{2,}[!\.?](?=[\.!?])'
    try_compiling = re.compile(add_to_abbreviations, re.IGNORECASE)
    #adding to ignored instances of punkt tokenizer
    def append_to_abbreviations(matchobj):
        punkt_param.abbrev_types.update(matchobj.group(0))
        return matchobj.group(0)
    #add to filter abbreviations
    re.sub(try_compiling, append_to_abbreviations, all_expanded)
    # print(list_of_matched_objects)
    substitute_pre_period = re.compile('|'.join(['%s'.lstrip() % re.escape(match + '.') for match in list_of_matched_objects]))
    if list_of_matched_objects:
        def remove_pre_period(matchobj):
            return matchobj.group(0)[:-1] + ' '
        abbreviation_sieve = re.sub(substitute_pre_period,remove_pre_period, all_expanded)
        return abbreviation_sieve
    else:
        return all_expanded

def wide_build_emoji(text_blob):
    def test_match_object(matchobj):
        matched = matchobj.group(0)
        # print (matched)
        return ''
    return re.sub(regex_compiled_emoji, test_match_object, text_blob)

def polish_(post_sourcing_text):
    effective_spacing = re.compile(r'(?!\.)[\.!?]')
    def add_effective_spaces(matchobj):
        return matchobj.group(0) + ' '
    sentence_ready = re.sub(effective_spacing, add_effective_spaces, post_sourcing_text)
    sentence_ready = re.sub(r' {2,}', ' ', sentence_ready)
    return sentence_ready


def source_for_dictionary(text_blob, d_obj, special_case='', regex_pattern = "%r"):
    if d_obj:
        source_file = {k.lower().strip() : v.lower().strip() for k,v in d_obj.items()}
        # escape_ = [regex_pattern % re.escape(ki) if ]
        standard_search = re.compile('|'.join([regex_pattern % re.escape(ki) if ki.count('.') >= 1 else regex_pattern % ki for ki in source_file.keys()]), re.IGNORECASE)
        if not special_case:
            def find_replace_value(matchobj):
                matched = matchobj.group(0)
                if matched != '':
                    try:
                        return source_file[matched]
                    except KeyError:
                        pass

                else:
                    return matchobj.group(0)
            return polish_(re.sub(standard_search, find_replace_value, text_blob))
        else:
            post_source = abbreviation_case_(text_blob, standard_search, source_file)
            return polish_(post_source)
    else:
        print ("No dictionary by that name")
        pass

def clean(text_blob):
        """Naive implementation of sequential same-alpha chars"""
        text_blob = text_blob.lower()
        strip_emoji = wide_build_emoji(text_blob)
        filter_surplus_white = re.sub(r'/s{2,}', ' ',strip_emoji)
        replace_at_symbol = re.sub(r'(?<=\s)@(?=\s)','at', filter_surplus_white)
        multi_seq_char_ex = r"\b.?(a{3,})|(e{3,})|(i{3,})|(o{3,})|(u{3,}).?\b|[^\s\w]{2,}|/s{2,}"
        def func(matchobj):
            matched = matchobj.group(0)
            only = [x for x in matched if x in string.punctuation][0:]
            y = {x: matched.count(x) for x in list(matchobj.group(0))}
            priority_punctuation = ['!','?','.']
            check_punct = [p for p in priority_punctuation if p in only]
            if check_punct:
                return check_punct[0]
            else:
                return ''.join(list(OrderedSet(matched)))
        no_multi_char = re.sub(multi_seq_char_ex, func, replace_at_symbol)
        special_case_number_pattern = re.compile(r'\bn(|o|um) ?\.|\bnum ')
        remove_number_prefixes_ = re.sub(special_case_number_pattern, 'number', no_multi_char)
        return remove_number_prefixes_


def check_wide_distance(tok):
    # reduces load
    # helper function
    try:

        correction = [k for k,v in main_dict['Wide_Distance_corrections']['read_in'].items() if tok.lower() == v.lower()][0]
        return correction
    except (KeyError, IndexError):
        return tok


def common_misspellings(sentence):
    """
    Spell check and secondary tokenizer
    """
    def hasNumbers(inputString):
        return any(char.isdigit() for char in inputString)

    common_tokens, common_acronyms = process_whatsapp(main_dict['whatsapp_messages']['read_in'])
    sentence = sentence.lower() #uniform sentence
    compile_string = ''
    store_preceding_token  = ''
    for token in word_tokenize(sentence):
        if token not in string.punctuation + '@':
            if token not in set(common_tokens.keys()).union(set(common_acronyms.keys())):
                if ENCHANT_VERIFY.check(token) is False:
                    if hasNumbers(token):
                        compile_string += token + ' '
                        #contains number
                    else:
                        if len(token) >= 4:
                            if norvig_candidates(token,1):
                                primary_candidates = norvig_candidates(token, 1)
                                same_first_token_candid = [i for i in list(primary_candidates) if i[0] == token[0]]
                                if same_first_token_candid:
                                    new_candidates_list = set(same_first_token_candid)
                                    compile_string  += max(new_candidates_list, key = norvig_probability) + ' '
                                else:
                                    if norvig_candidates(token,2):
                                        secondary_candidate = norvig_candidates(token, 2)
                                        same_first_char_candid = [ii for ii in list(secondary_candidate) if ii[0] == token[0]]
                                        if same_first_char_candid:
                                            new_candidates_list = set(same_first_char_candid)
                                            compile_string  += max(new_candidates_list, key = norvig_probability) + ' '
                                    else:
                                        compile_string += max(primary_candidates, key = norvig_probability) +  ' '
                            else:
                                if norvig_candidates(token,2):
                                    secondary_candidates = norvig_candidates(token,2)
                                    same_tok_candid = [xx for xx in list(secondary_candidates) if xx[0] == token[0]]
                                    if same_tok_candid:
                                        compile_string += max(set(same_tok_candid), key = norvig_probability) + ' '
                                    else:
                                        compile_string += max(secondary_candidates, key = norvig_probability) + ' '
                                else:
                                    compile_string += token + ' '
                        else:
                            if token not in stop:
                                #avoid
                                compile_string += norvig_correction(store_preceding_token, token) + ' '
                            else:
                                compile_string += token + ' '

                            #using autocomplete candidates
                else:
                    compile_string += token + ' '
            else:
                compile_string += token + ' '
        else:
            compile_string += token + ' '
        store_preceding_token = token
    if compile_string:
        compile_string = compile_string.strip()
        if compile_string[-1] not in ['!', '?', '.']:
            return compile_string + '.'
        else:
            return compile_string

tokenizer = PunktSentenceTokenizer(punkt_param)

def sentence_tokenizer(text_blob):
    clean_text = clean(text_blob)
    parse_priority_tokens = source_for_dictionary(clean_text, main_dict['Priority_tokens']['read_in'], regex_pattern = r'(?<=\b)%s(?=[!\. \?])(?!\.\w)')
    parse_abbreviations = source_for_dictionary(parse_priority_tokens, main_dict['Abbreviations_Dictionary']['read_in'], 'yes', regex_pattern = r'(?<=\b )%s(?=( \b|[\.!\?]))')
    parse_colloqualisms = source_for_dictionary(parse_abbreviations, main_dict['SMS_Colloqualisms']['read_in'], regex_pattern = r'(?<=\b)%s(?=[!\. ?])')
    expand_contractions = ' '.join(str(main_dict['Contractions']['read_in'].get(word, word)) for word in parse_colloqualisms.split())
    correct_spellings = [common_misspellings(sent) for sent in tokenizer.tokenize(expand_contractions)]
    return correct_spellings

def non_word_dig_char(word):
    return ''.join(chr for chr in word if chr not in string.punctuation).strip()

greeting_table = pd.read_csv(script_path + 'greeting_candidates_list.csv')
greeting_list = list(map(non_word_dig_char, greeting_table['greeting']))
multi_tok_greet = list(map(str.lower, list(filter(lambda x: len(x) > 3, greeting_list))))
single_tok_greet = list(map(str.lower, list(filter(lambda x: len(x) <= 3, greeting_list))))

def is_greeting(sentence):
    """
    returns 1 for contained -notice past tense -  i.e function removes greeting.
    """
    lower_case_sentence = list(map(lambda x: x.strip(), ''.join(char.lower() for char in sentence).split(',')))
    candidate_classification = []
    single_token_counter = 0
    for item in lower_case_sentence:
        candidates = []
        compile_new_string = ''
        token_word = word_tokenize(item)
        for index, tok in enumerate(token_word):
            if tok not in string.punctuation:
                if any(x == non_word_dig_char(tok.lower()) for x in single_tok_greet) == True:
                    single_token_counter += 1
                    candidates.append(tok)
                else:
                    compile_new_string = compile_new_string + ' ' + tok
                    for greeting in multi_tok_greet:
                        if fuzz.ratio(greeting, compile_new_string.lstrip()) > 85:
                            candidates.append(compile_new_string)
            else:
                compile_new_string  = compile_new_string + tok
        try:
            candidates = sorted(candidates,key=len, reverse=True)[0].strip()
            candidate_classification.append((candidates, 'gr'))
        except IndexError:
                candidate_classification.append((item, ''))
    sentence_candidates = [x[0] for x in candidate_classification if x[1] == 'gr']
    candidates_pattern = re.compile('|'.join(sentence_candidates), re.IGNORECASE)
    remove_candidates = re.sub(candidates_pattern,'', sentence)
    remove_stop = ' '.join(word for word in word_tokenize(remove_candidates) if word not in stop and word not in string.punctuation)
    modify_out_string = ''.join([char for char in remove_candidates if char not in [',', '-']])
    if len(modify_out_string) > 1:
        modify_out_string = modify_out_string[0].upper().lstrip() + modify_out_string[1:].rstrip()
        if len(word_tokenize(remove_stop)) <= 1 and len(sentence_candidates) >= 1:
            #has just one or less token left in sentence after removing stop words
            return (1, '')
        else:
            if len(candidates) == 0:
                #no greetings found
                return (None, modify_out_string)
            else:
                #greetings found and sentence parsed
                return (1, modify_out_string)
    else:
        return (1, modify_out_string)


def spell_checker(text_blob):
    """
    One function to rule them all
    """
    add_punc = text_blob
    if text_blob[-1] not in ['!','.', '?']:
        add_punc = add_punc + '.'
    sentence_tokens = (sentence_tokenizer(add_punc))
    if sentence_tokens:
        message_for_nlp = []
        greeting_counter = 0
        for sent in sentence_tokens:
            greeting_callback = is_greeting(sent)
            if greeting_callback:
                if greeting_callback[0] == 1:
                    greeting_counter += 1
                    if greeting_callback[1]:
                        message_for_nlp.append(greeting_callback[1])
                else:
                    if greeting_callback[1]:
                        message_for_nlp.append(greeting_callback[1])
        if greeting_counter:
            call_greeting_bot = '' #send function here
        return message_for_nlp
    elif text_blob == '':
        #no message received, default to greeting
        call_greeting_bot = ''
        pass


# if __name__ == '__main__':
# print (spell_checker('I am a happy walrus'))



