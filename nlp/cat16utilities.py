from collections import Counter
import csv
import re
import pickle
from datetime import datetime, date, timedelta
import pandas as pd
import pymongo
import json
import random
import numpy as np
from sklearn.model_selection import train_test_split

import nltk.classify.util
from nltk.corpus import stopwords
import nltk.tag

random.seed(9001)
cat16_flags=['fare_orig_city' ,
 'fare_dest_city' ,
 'orig_fare_amt' ,
 'orig_fare_curr' ,
 'cat16_cat_id' ,
 'appl_vol' ,
 'appl_invol' ,
 'appl_canx' ,
 'tkt_non_rfnd', 
 'pen_canx' ,
 'pen_noshow', 
 'pen_reiss' ,
 'pen_lost' ,
 'pen_reval' ,
 'pen_rfund' ,
 'pen_pta' ,
 'pen_geo_t995', 
 'chrg_appl' ,
 'chrg_prt' ,
 'chrg_amt1' ,
 'chrg_curr_cd1', 
 'chrg_amt2' ,
 'chrg_curr_cd2', 
 'chrg_pct' ,
 'chrg_high_low', 
 'waive_death' ,
 'waive_ill' ,
 'waive_death_fam', 
 'waive_ill_fam' ,
 'waive_sc' ,
 'waive_tkt_upg', 
 'cat16_dt_ovrd_t994', 
 'cat16_txt_tbl_t996' ,
 'cat16_unavl_ind']

# used to transfer CSV files to MongoDB
def import_content(filepath,collection_name, mng_client):
    mng_db = mng_client['raxdb'] # Replace mongo db name
    
    db_cm = mng_db[collection_name]
    #cdir = os.path.dirname(__file__)
    #file_res = os.path.join(cdir, filepath)

    #data = pd.read_csv(file_res)
    data = pd.read_csv(filepath)
    data2=data.fillna('')
    data_json = json.loads(data2.to_json(orient='records'))
    db_cm.drop()
    db_cm = mng_db[collection_name]
    db_cm.insert_many(data_json)

#class util16():
def hello2():
    print('Hello World from cat16utilities')

#specifically for importing SAMPLE CAT16 CSV files
def import_content_CAT16(filepath,collection_name, mng_client):
    mng_db = mng_client['raxdb'] # Replace mongo db name
    
    db_cm = mng_db[collection_name]
    #cdir = os.path.dirname(__file__)
    #file_res = os.path.join(cdir, filepath)

    #data = pd.read_csv(file_res)
    data = pd.read_csv(filepath, delimiter=',', dtype=
                       {"appl_canx": str, 
                        "tkt_non_rfnd": str,
                        "pen_noshow": str, 
                        "pen_reiss": str, 
                        "chrg_high_low": str, 
                        "waive_death": str, 
                        "waive_ill": str, 
                        "waive_death_fam": str, 
                        "waive_ill_fam": str, 
                        "waive_tkt_upg": str,
                        "cat16_unavl_ind": str,
                        "cat16_txt_tbl_t996": int,
                        "chrg_amt1": int,
                        "chrg_amt2": int, 
                        "chrg_curr_cd2": str,
                        "fee_appl_type": str,
                        "pen_min_curr": str,
                        "pen_min_curr2": str,
                        "pen_high_low": str,
                        "pen_min_amt": int,
                        "pen_min_amt2": int
                       }, low_memory=False)
    data2=data.fillna(False)
    data_json = json.loads(data2.to_json(orient='records'))
    db_cm.drop()
    db_cm = mng_db[collection_name]
    db_cm.insert_many(data_json)


def import_content_with_headers(filepath,collection_name, mng_client):
    mng_db = mng_client['raxdb'] # Replace mongo db name
    
    db_cm = mng_db[collection_name]
    #cdir = os.path.dirname(__file__)
    #file_res = os.path.join(cdir, filepath)

    #data = pd.read_csv(file_res)
    f = open(filepath, 'r')
    db_cm.drop()
    reader = csv.DictReader(f)
    headers=next(reader, None)  
    for each in reader:
        row={}
        for field in each:
            row[field]=each[field]
        db_cm.insert_one(row)

# >> list2string(['CNY', '100', 'EQUIVALENT'])
# >> 'CNY 100 EQUIVALENT'

def list2string(wordlist):
    res=''
    for word in wordlist:
        res += word +' '
    return res.strip()


##### UTILITIES TO HANDLE NUMBERS AND CURRENCIES IN TEXT PROCESSING
def splitCurrency(word, currencycodes):
    if len(word)<4:
        return word
    else:
        if word[:3].upper() in currencycodes                         :
            return [word[:3].upper(), word[3:]]
        else:
            return word

def normalise_list(wordlist):
    ls=[]
    for word in wordlist:
        if isinstance(word, str):
            ls.append(word)
        else:
            for w in word:
                ls.append(w)
    return ls

def fix_currencies(wordlist,currencycodes):
    return normalise_list([splitCurrency(word,currencycodes) for word in wordlist])


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass
 
    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
 
    return False

def preprocessCAT16rules(line, currencycodes):
    #print('- fname', fname)
    stoplist = set(stopwords.words("english"))
    stoplist.discard('no') #remove 'no' from stop list
    stoplist.discard('not') #remove 'no' from stop list
    stoplist.discard('before')
    stoplist.discard('after')
    #stoplist.add('apply')
    #stoplist.add('applicable')
    #stoplist.add('applies')
    #stoplist.add('payable')
    #stoplist.add('applied')
    #stoplist.add('HTML')
    STOPLIST= [x.upper() for x in stoplist]
    percentage = re.compile('\d+%')
    ddmon=re.compile('\d+[JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC]')
    sentence_end = re.compile(r'\w*[:|,|"|&|\.]')
    translation_table = dict.fromkeys(map(ord, r'()!@#$"“”````'), None)
    translation_table2 = dict.fromkeys(map(ord, ":,'"), ' ')
    translation_table3 = dict.fromkeys(map(ord, '/'), ' / ')
    translation_table4 = dict.fromkeys(map(ord, '-'), ' - ')
    #pc=re.compile(r'commission\w*|commision\w*|comission\w*')
    pc=re.compile(r'COMMISSION\w*|COMMISION\w*|COMISSION\w*')
    result=[]
    #shortforms={'issd':'sales', 'trvl': 'travel', 'eff':'period', 'owl':'ONE WORLD ALLIANCE'} #NEED GUI TO AUGMENT THIS LIST OVER TIME
    shortforms={'ISSD':'SALES', 'TRVL': 'TRAVEL', 'EFF':'EFFECTIVE', 'Eff':'EFFECTIVE','OWL':'ONE WORLD ALLIANCE'} #NEED GUI TO AUGMENT THIS LIST OVER TIME 
    nltksentences = nltk.sent_tokenize(line) 
    sentences=[]
    for sentence in nltksentences:
        sentences += sentence.split(r'&')
        
    for sentence in sentences: #, encoding="utf-16"  , encoding="latin-1"
        #this is a preprocessor
        #texts = line.rstrip("\n").decode("utf-16")
        #texts = line.upper().split() #change to upper case 
        texts = nltk.word_tokenize(sentence)
        #word_tokenize splits % from number
        #texts = [percentage.match(word).group(0)  if percentage.match(word) else word for word in texts] #ensure percentages are separate numbers
        texts = [word.translate(translation_table) for word in texts] #drop parens and other odd characters
        texts = [word.translate(translation_table2) for word in texts] #ensure sentence and word boundary typos are fixed along the way
        texts = [word.translate(translation_table3) for word in texts] #insert spaces between / so routes can be recognised
        texts = [word.translate(translation_table4) for word in texts]
        
        texts = [word for word in texts if word != '--']
        #texts = [word if word in TOURCODES else re.split('(\d+)',word) for word in texts ] #break up dates MMMyy -> MMM yy
        #texts = flattern(texts)
        texts = [word.strip() for word in texts] # remove leading and trailing spaces
        texts = [word for word in texts if word not in STOPLIST]
        #texts = [shortforms[word] if word in shortforms.keys() else word for word in texts ]
        #texts = ['COMMISSION' if pc.match(word) else word for word in texts] #clean up the use of the word commission
        #texts = flatten(texts) #catch renegade strings that should be split again
        texts = [word for word in texts if word]
        texts = fix_currencies(texts, currencycodes) #split currency code and amount
        #texts = [spellchecker(word) for word in texts if word]
        result += [texts]
    return result

def break_notes(wordlist):
    res=[]
    sub=[]
    for w in wordlist:
        if w.isdigit():
            res.append(sub)
            sub=[]
            continue
        else:
            sub.append(w)
    return res

########   PROCESSING   ######################################



def processCAT16_wordlist(db, collection, reprocess):
    #create paragraphs with processed text - useful for full text search
    t1=datetime.now()
    cnt=0
    cat16=db[collection]
    #reissues=cat16.find({'appl_vol':'X', 'pen_reiss':'X'})
    #cat16_records=cat16.find({"_id": ct16['_id'] })
    if reprocess:
        cat16_records=cat16.find().batch_size(5000)
    else:
        cat16_records=cat16.find({ 'word_tokens_list': { '$exists': False } }).batch_size(5000) #set batch size for number of docs to be processed in 10 min to avoid CursorNotFound
    total=cat16_records.count()
    all_words=[]
    l_all_words=[]
    whole_txt_dict={}
    line_txt_dict={}
    for rule in cat16_records:
        cnt+=1

        full_sents=''
        full_para=''
        if rule['fare_id'] in whole_txt_dict:
            word_tokens_list,wordlist = whole_txt_dict[rule['fare_id']]
        else:
            word_tokens_list,wordlist=  create_wordlist(db,rule['whole_txt']) 
            whole_txt_dict[rule['fare_id']]=( word_tokens_list,wordlist)
        
        if rule['cat16_txt_tbl_t996'] in line_txt_dict:
            l_word_tokens_list,l_wordlist= line_txt_dict[rule['cat16_txt_tbl_t996']]
        else:
            l_word_tokens_list,l_wordlist=  create_wordlist(db,rule['line_txt']) 
            line_txt_dict[rule['cat16_txt_tbl_t996']]=(l_word_tokens_list,l_wordlist)
 
        #print(word_tokens_list)
        # for sent in word_tokens_list:
        #     full_sents = ' '.join(map(str, sent))
        #     full_para += full_sents + '\n'

        cat16.update_one({"_id": rule['_id'] },{'$set' : {  'word_tokens_list' : word_tokens_list,
                                                            'wordlist' : wordlist,
                                                            'l_word_tokens_list':l_word_tokens_list,
                                                            'l_wordlist': l_wordlist
                                                             }})
        # cat16.update_one({"_id": rule['_id'] },{'$set' : {  'l_word_tokens_list':l_word_tokens_list,
        #                                                     'l_wordlist': l_wordlist
        #                                                      }})
        all_words+=wordlist
        l_all_words+= l_wordlist
        if (cnt % 1000) == 0:
            t3=datetime.now()
            cum=(t3-t1).total_seconds()
            rem=((total-cnt)*cum/cnt)/60
            etf=(t3+timedelta(minutes=rem)).strftime('%H:%M')
            print(cnt, ' faresheets processed')
            stat=f"{cum:.1f} seconds so far. WORDLIST estimated to complete in {rem:.1f} minutes at {etf}"
            print(stat)
    return (all_words,l_all_words)

def create_wordlist(db,txt_field):
    currencycodes= set([code['currencycode']  for code in db.currency.find()])
    word_tokens_list= preprocessCAT16rules(txt_field,currencycodes)
    wordlist=[ word 
              for sublist in word_tokens_list
              for word in sublist 
              ]
    w=wordlist
    w2=normalise_list(list(map(lambda x: x.split('-'), w)))
    w3=normalise_list(list(map(lambda x: x.split('/'), w2)))
    w4=normalise_list(list(map(lambda x: x.split('.'), w3)))
    w5=normalise_list(list(map(lambda x: x.split(','), w4)))
    w6=normalise_list(list(map(lambda x: x.split(' '), w5)))
    #w6=normalise_list(list(map(lambda x: x.split('，'), w5))) #unusual comma character found in CX faresheets
    wt = []
    for temp in w6:
        string = re.sub("[+——！，。？、~@#￥%……&*""《》（）]+", ",",temp)
        string = string.split(',')
        string= list(filter(lambda item:item.strip(),string))
        if isinstance(string,list):
            for s in string:
                wt.append(s)
        else:
            wt.append(string)
    wordlist=[word.strip() for word in wt if word.strip() !='' ]
    wordset=list(set(wordlist))
    return (wordset, wordlist)

# for updating wordlist
def refresh_wordlist(db,faresheets, search_dict={}):
    t1=datetime.now()
    run=int(t1.strftime('%Y%m%H%M'))
    cnt=0
    fs=db[faresheets]
    total=fs.find(search_dict).count()
    print('Run:',run, '-', total,'files to be updated')
    for fare in fs.find(search_dict):
        #print('Processing: ', fare['filepath'])
        wordset, wordlist=create_wordlist(db,fare['teststring'])
        try:
            fs.update_one({'_id':fare['_id']}, {'$set': {  'run':run, 
                                            'wordset':wordset,
                                            'wordlist':wordlist}})
        except pymongo.errors.DocumentTooLarge:
            print('DocumentTooLarge error while processing ', fare['filepath'])
            continue

        cnt+=1
        if (cnt % 1000) == 0:
            t3=datetime.now()
            cum=(t3-t1).total_seconds()
            rem=((total-cnt)*cum/cnt)/60
            etf=(t3+timedelta(minutes=rem)).strftime('%H:%M')
            print(cnt, ' faresheets processed')
            stat=f'{cum:.1f} seconds so far. WORDLIST estimated to complete in {rem:.1f} minutes at {etf}'
            print(stat)
    print(cnt, ' faresheets processed')

def longest_word_in_wordlist(wordlist):
    m=0
    for w in wordlist:
        m=max(m,len(w))
    return m
############ SPLIT DATASETS into Training, Dev-Test and Production



####### splits data into 'training', 'dev_test' and 'production'
####### to use   >>    processCAT16_train_test_split(db.sample7)
def processCAT16_train_test_split(sample):
    t1=datetime.now()
    f=sample.find({'training':True})
    rec_id=[rec['_id'] for rec in f]
    data=np.array(rec_id)
    #x_train ,x_prod = train_test_split(data,test_size=0.5) 
    x_train ,x_prod = train_test_split(data,test_size=0.5, random_state=42)
    dev_no= int(round(len(x_train)*2/3,-2))
    print('total cases:', f.count())
    print('training+dev_test:',dev_no)
    x_dev=x_train[:dev_no]
    x_dev_test=x_train[dev_no:]
    sample.update_many({'_id': {'$in': list(x_dev)} },{'$set': {'cases':'training'}})
    sample.update_many({'_id': {'$in': list(x_dev_test)} },{'$set': {'cases':'dev_test'}})
    sample.update_many({'_id': {'$in': list(x_prod)} },{'$set': {'cases':'production'}})

    t8=datetime.now()
    bench=(t8-t1).total_seconds()/60
    msg=f'TOTAL processing time: {bench:.1f} minutes'
    print(msg)



############# FEATURES processing  ###############

def is_cat16flag(x):
    return (x in cat16_flags)

def processCAT16_features(db, sample):
    t1=datetime.now()
    all_words = []
    cat16=db[sample]
    cat16_records=cat16.find().batch_size(5000)
    for rule in cat16_records:
        all_words+=rule['l_wordlist']
    all_words = nltk.FreqDist(all_words)
    word_features = list(all_words.keys())
    cat16_flags=['fare_orig_city' ,
     'fare_dest_city' ,
     'orig_fare_amt' ,
     'orig_fare_curr' ,
     'cat16_cat_id' ,
     'appl_vol' ,
     'appl_invol' ,
     'appl_canx' ,
     'tkt_non_rfnd', 
     'pen_canx' ,
     'pen_noshow', 
     'pen_reiss' ,
     'pen_lost' ,
     'pen_reval' ,
     'pen_rfund' ,
     'pen_pta' ,
     'pen_geo_t995', 
     'chrg_appl' ,
     'chrg_prt' ,
     'chrg_amt1' ,
     'chrg_curr_cd1', 
     'chrg_amt2' ,
     'chrg_curr_cd2', 
     'chrg_pct' ,
     'chrg_high_low', 
     'waive_death' ,
     'waive_ill' ,
     'waive_death_fam', 
     'waive_ill_fam' ,
     'waive_sc' ,
     'waive_tkt_upg', 
     'cat16_dt_ovrd_t994', 
     'cat16_txt_tbl_t996' ,
     'cat16_unavl_ind',
     'jrny_ind'
     ]
     # next bit should be data driven
    # inf_words=[['INFANT', 'INF'], ['CHILD']]  #make this a list of synonyms, with the first word being the tag header
    # inf_search=[(['WITHOUT','SEAT','NO','FEE'],[]) ,
    #         (['WITHOUT','SEAT','DISCOUNT','APPLIES'],[]),
    #         (['SEAT','DISCOUNT','APPLIES'],['WITHOUT']),
    #         (['NO','WITH','SEAT','DISCOUNT','APPLIES'],[])] 
    # infant_field_list=create_field_list(inf_words, inf_search)                               
    # words=[['COLLECT']]
    # search=[([ 'CHANGE', 'FEE'],[]) ]
    # field_list=create_field_list(words, search)
    # cat16_flags=cat16_flags+infant_field_list+field_list
     #'jrny_ind']  #include jrny_ind as an input
    #cat16_feature_items=cat16_flags + word_features
    cat16_feature_items=cat16_flags #include only True words dynamically when processing ie ignore False values
    # backup old feature set and drop table
    backup=backup_collection(db,'cat16_features')
    print('cat16_features backed up as: ', backup)
    db.cat16_features.drop()

    features16=db.cat16_features
    currencycodes= set([code['code']  for code in db.currency.find()])
    features16.insert_many([ {'field':x,
                              'number': is_number(x),
                             'cat16_flag': is_cat16flag(x),
                             'currency': x in currencycodes }
                            for x in cat16_feature_items])
    db.cat16_features.update_many({'field':{'$ne': 'chrg_appl'}}, {'$set':{'jrny_ind':False}}) #limit features for jrny_ind to chrg_appl only
    cat16.update_many({'chrg_appl': {'$eq':1}},{'$set':{'jrny_ind_calc':'AB'}})
    cat16.update_many({'chrg_appl': {'$eq':4}},{'$set':{'jrny_ind_calc':'AB'}})
    cat16.update_many({'chrg_appl': {'$eq':0}},{'$set':{'jrny_ind_calc':'AB'}})
    cat16.update_many({'chrg_appl': {'$eq':3}},{'$set':{'jrny_ind_calc':'A'}})
    cat16.update_many({'chrg_appl': {'$eq':2}},{'$set':{'jrny_ind_calc':'B'}})

    # helpful features for fee_appl_cd
    txts=['CHARGE THE HIGHEST CHANGE FEE  OF ALL FARE COMPONENTS WITHIN  ALL CHANGED PRICING UNITS']
    txts+=['HIGHEST FEE OF ANY CHANGED FARE COMPONENT  WITHIN JOURNEY WILL APPLY']
    [create_flag_field(db,cat16,txt) for txt in txts]
    print('fee_appl_cd features created')
    
    #helpful for disc_tag1 and disc_tag2
    txts=['CHILD/INFANT WITH SEAT -  DISCOUNT APPLIES TO CHANGE FEE']
    txts+=['CHILD/INFANT WITH SEAT -  NO DISCOUNT APPLIES TO CHANGE FEE']
    txts+=['INFANT WITHOUT SEAT-FREE OF CHARGE']
    txts+=['INFANT WITHOUT SEAT - NO CHANGE FEE']
    txts+=['REROUTING FEE IS FOC']
    txts+=['COLLECT CHANGE FEE']
    [create_flag_field(db, cat16,txt) for txt in txts]
    print('disc_tag1 and disc_tag2 features created')

    #helpful for residual_pen 
    txts=['NEW ITINERARY RESULTS IN A LOWER FARE- NO REFUND OF FARE DIFFERENCE'] #value I
    txts+=['NEW ITINERARY RESULTS IN A LOWER FARE NO REFUND OF THE RESIDUAL AMOUNT WILL BE MADE'] #value I
    txts+=['WHEN THE NEW ITINERARY RESULTS IN A LOWER FARE- REFUND FARE DIFFERENCE -AND- COLLECT CHANGE FEE IF APPLICABLE'] #
    txts+=['NEW TKT MUST BE EQUAL OR HIGHER VALUE THAN PREVIOUS TKT']
    [create_flag_field(db, cat16,txt) for txt in txts]
    print('residual_pen features created')

    print('New features created. Feature tagging started.')
    # tag which features will be used for which CAT31 fields
    cat31fields=db.sample_scope.distinct('fields')
    curr_fields=['pen_curr1','pen_curr2']
    general=[f for f in cat31fields if f not in curr_fields ] #everything but currency fields

    # all CAT16 fields will use the CAT16 flags
    [db.cat16_features.update_many({'cat16_flag': True}, {'$set': {f : True}}) for f in cat31fields]

    # general (ie non-currency) fields will exclude numeric features
    [db.cat16_features.update_many({'number': False}, {'$set': {f : True}}) for f in general]

    # currency fields will be reduced to CAT16 flags + currencycodes
    curr_codes= list(set([ feat['code' ] for feat in db.currency.find()]))
    [db.cat16_features.update_many({'field': { '$in': curr_codes }}, {'$set': {f : True}}) for f in curr_fields]
    
    print('Feature tagging completed')
    t5=datetime.now()   
    bench2=(t5-t1).total_seconds()/60
    msg=f'TOTAL FEATURES processing time: {bench2:.1f} minutes'
    print(msg)

def add_new_feature(db, new_feature):
    #add one more feature which will be used for all CAT31 fields
    new_entry=db.cat16_features.insert_one({'field': new_feature, 'cat16_flag': True})
    cat31fields=db.sample_scope.distinct('fields')
    # all CAT16 fields will use the CAT16 flags
    new=db.cat16_features.find_one({'field': new_feature})
    [db.cat16_features.update_one(new, {'$set': {f : True}}) for f in cat31fields]

def find_features(document,word_features):
    words = set(document)
    features = {}
    for w in word_features:
        features[w] = (w in words)

    return features

from itertools import  count
def ngram(n,word, wordlist):
    l=[i for i, j in zip(count(), wordlist) if j == word] # find all occurances of `word`
    feat={}
    for j,i in zip(count(),l):

        context_list=break_notes(wordlist[(i-n) : (i+n)]) #find the words excluding numbers

        context_list=[note for note in context_list if word in note]
        #print(context_list)
        if context_list==[]:
            continue
        for context in context_list:
            context=set(context)
            #print(context)
            # if set(['WITHOUT','SEAT'])< context:
            #     feat[word+'_WITHOUT_SEAT']=True
            # else:
            #     feat[word+'_WITHOUT_SEAT']=False

            # if set(['WITH','SEAT'])< context:
            #     feat[word+'_WITH_SEAT']=True
            # else:
            #     feat[word+'_WITH_SEAT']=False

            # if set(['NO','FEE'])< context:
            #     feat[word+'_NO_FEE']=True
            # else:
            #     feat[word+'_NO_FEE']=False

            # if set(['DISCOUNT','APPLIES'])< context:
            #     feat[word+'_DISCOUNT']=True
            # else:
            #     feat[word+'_DISCOUNT']=False

            if set(['WITHOUT','SEAT','NO','FEE'])< context:
                feat[word+'_WITHOUT_SEAT_NO_FEE']=True

            if set(['WITHOUT','SEAT','DISCOUNT','APPLIES'])< context:
                feat[word+'_WITHOUT_SEAT_DISCOUNT']=True

#            if set(['SEAT','DISCOUNT','APPLIES'])< context:
            if set(['SEAT','DISCOUNT','APPLIES'])< context and not(set(['WITHOUT'])<context):
                feat[word+'_WITH_SEAT_DISCOUNT']=True

        # for i,w in zip(count(),before):
        #     feat[str(j)+ word +'_B'+ str(i)]=w
        # for i,w in zip(count(),after):
        #     feat[str(j)+ word +'_A'+ str(i)]=w  
    return feat

def ngram_list(n,synwords, wordlist, search_list_of_lists):
    l=[]
    for word in synwords: #find the position of the synwords in the wordlist. Only one should fire
        temp=[i for i, j in zip(count(), wordlist) if j == word]
        if temp:
            l=temp

    feat={}
    #print('l',l)
    syn=synwords[0]
    for word in synwords:
        #print('syn:', word)
        for j,i in zip(count(),l):
            #print('j',j,'i',i)
            context_list=break_notes(wordlist[(i-n) : (i+n)])

            context_list=[note for note in context_list if word in note]
            #print('context_list' , context_list)
            if context_list==[]:
                continue
            for context in context_list:
                context=set(context)
                #print('context: ',  context)
                for search,not_search in search_list_of_lists:
                    if not_search:
                        for no in not_search:
                            if set(search) < context and (set([no])<context):
                                feat[syn +'_' +no+'_'+ ('_').join(search)] = True  #set the feature
                        # else:
                        #     feat[word +'_' + ('_').join(search)] = False  #feature absent
                    else:
                        #print('search:', search)
                        if set(search) < context:
                            #print('found')
                            #feat[syn +'_' +('_').join(search)] = True #set the feature
                            yes,no=search_list_of_lists[0]
                            feat[syn +'_' +('_').join(yes)] = True #map the feature to the first
                            #print(feat)
                        # else:
                        #     print('not found')
                        #     feat[word +'_' +('_').join(search)] =False #feature absent
    return feat


def create_field_list(who_syn_list, searchlist):
    field_list=[]
    for synlist in who_syn_list:
        word=synlist[0]
        for search,not_search in searchlist:
            if not_search:
                for no in not_search:
                    field_list.append(word +'_X_' +no +'_'+ ('_').join(search))
            else:
                   field_list.append(word +'_' + ('_').join(search)) 
    return field_list

def create_flag_field(db,cat16,txt):
    txt=' '.join(txt.split())
    search_txt="\"" + txt + "\""
    tag='_'.join(txt.split())
    if list(cat16.find({tag: {'$exists':True}})):
        cat16.update_many({},{'$unset': {tag:0}}) #remove the new field

    #found=cat16.find( { "$text": { "$search": search_txt } },{'_id':1} )
    cat16.update_many({ "$text": { "$search": search_txt } }, 
                        {'$set': {tag:True}})
    cat16.update_many({tag:{'$ne':True}}, 
                        {'$set': {tag:False}}) #add False attribute to avoid KeyErrors in other code
    add_new_feature(db, tag)
    #print(found.count(),' records updated')
    return True

########## NOT SO USEFUL ######################
# def find_features_bow_count(document):
#     # extends features to include count of the words in the document
#     document = [w for w in document if w not in ['-','.']]
#     words = set(document)
#     word_count=Counter(document)
#     features = {}
#     for w in word_features:
#         features[w] = word_count[w]
#
#     return features
#
# def find_fare_features(fare, feature_items_list):
#     #return a dictionary of features
#     #22/1/18 using the count of words DECREASED accuracy; most likely by framenting the base data
#     #so going back to TRUE/FALSE
#     features={}
#     #feature_items_list =list(filter(lambda x: not(is_number(x)), feature_items_list)) #remove numbers
#     for feature_item in feature_items_list:
#         #features[feature_item] = fare[feature_item] #run 2 where this is the count of the word
#         val=fare[feature_item]
#         try:
#             cnt = int(val)
#             features[feature_item] = True if int(cnt)>0 else False # go back to True/False
#         except TypeError:
#             features[feature_item] = val #for CAT16 flag items
#         except ValueError:
#             features[feature_item] = val #for CAT16 flag items
#
#     return features
#
# def get_word_features(wordlist):
#     feat={}
#     for w in wordlist:
#             feat[w]=True
#     return feat
########## END OF NOT SO USEFUL ###################### 

# add BOW features  to faresheets
# to use >>  processCAT16_add_BOW_features(db.sample7, db.cat16_features)
def processCAT16_add_BOW_features(cat16, features):
    t1=datetime.now()
    ####################################
    cnt=0
    #cat16_records=cat16.find({'cases':'production'})
    cat16_records=cat16.find({'cases':{'$in':['training','dev_test']}})
    #cat16_feature_items = [ feat['field' ] for feat in features.find({'number': False,'cat16_flag':False})]
    total=cat16_records.count()
    whole_txt_dict={}
    line_txt_dict={}
    label_feats = collections.defaultdict( list)

    for rule in cat16_records:


        if rule['fare_id'] in whole_txt_dict:
            bow_features= whole_txt_dict[rule['fare_id']]
        else:
            #bow_features=find_features(rule['wordlist'],cat16_feature_items) #boolean
            bow_features=get_word_features(rule['wordlist'])
            whole_txt_dict[rule['fare_id']]=bow_features
        cat16.update_one({"_id": rule['_id'] },{'$set' :  {'whole_txt_features':bow_features}})

        if rule['cat16_txt_tbl_t996'] in line_txt_dict:
            bow_features= line_txt_dict[rule['cat16_txt_tbl_t996']]
        else:
            #bow_features=find_features(rule['l_wordlist'],cat16_feature_items) #boolean 
            bow_features=get_word_features(rule['l_wordlist'])
            line_txt_dict[rule['cat16_txt_tbl_t996']]=bow_features
        cat16.update_one({"_id": rule['_id'] },{'$set' :  {'line_txt_features':bow_features}})

        #bow_features=find_features_bow_count(rule['wordlist']) #count of word usage => 22Jan18 abandon as it fragments Bayes calculations and reduces accuracy
        #cat16.update_one({"_id": rule['_id'] },{'$set' :   {'disc_tag1_guess_bow_count':guess }} )
        cnt+=1
        if (cnt % 1000) == 0:
            t3=datetime.now()
            cum=(t3-t1).total_seconds()
            rem=((total-cnt)*cum/cnt)/60
            etf=(t3+timedelta(minutes=rem)).strftime('%H:%M')
            print(cnt, ' rule records processed')
            stat=f'{cum:.1f} seconds so far. Estimated to complete in {rem:.1f} minutes at {etf}'        
            print(stat)

                         
    ####################################
    t4=datetime.now()
    bench=(t4-t1).total_seconds()/60
    msg=f'TOTAL processing time: {bench:.1f} minutes'
    print(msg)


def add_new_cat16flagfield(db,new_feature):
    new_entry=db.cat16_features.insert_one({'field': new_feature, 'cat16_flag': True})
    cat31fields=db.sample_scope.distinct('fields')
    # all CAT#! fields will use the CAT16 flags
    new=db.cat16_features.find_one({'field': new_feature})
    [db.cat16_features.update_one(new, {'$set': {f : True}}) for f in cat31fields]

def add_new_classification(db,category, feature_items_list):
    for item in feature_items_list:
        feat=db.cat16_features.find_one({'field':item})
        db.cat16_features.update_one(feat, {'$set': {category:True}})


def generate_features(rule,category,feature_items_list,currencycodes):
    # if rule['_id']==ObjectId('5a7b8b5eff0fd227f7142a71'):
    #     print('rule', rule['_id'])
    #     print(category)
    #     print(feature_items_list)
    features={feat : rule[feat] for feat in feature_items_list  }
    raw_wordlist=rule['l_wordlist']
    wordlist=raw_wordlist
    if category in ['pen_curr1','pen_curr2']:
        wordlist =[ word for word in raw_wordlist if word in currencycodes]  # reduce feature list to just currency codes
    if category in ['jrny_ind','jrny_ind_calc']:   #, 'disc_tag1', 'disc_tag2' combined features is best for disc_tags
        wordlist=[]
    if category in ['disctag9','disctagNOA','disctag8', 'disctags','disctags_9_NOA']:
        #wordlist=rule['l_wordlist']
        wordlist=rule['wordlist']
    # if category in ['disctag5']:
    #     wordlist=[]
        #wordlist=[]
    features.update(get_word_features(wordlist))

    # if category in ['disc_tag1', 'disc_tag2','disctag5', 'disctag8', 'disctag9', 'disctagNOA']:
    #     words=['INFANT', 'INF', 'CHILD']
    #     infant_search=[(['WITHOUT','SEAT','NO','FEE'],[]) ,
    #                     (['WITHOUT','SEAT','DISCOUNT','APPLIES'],[]),
    #                     (['SEAT','DISCOUNT','APPLIES'],['WITHOUT','NO'])]  #list of tuples with must have search and don't have search terms
    #     n=8
    #     for word in words:
    #         infant_features= ngram_list(n,word, raw_wordlist,infant_search)

    #         features.update(infant_features)

# #    if category in ['endorse_ind']:
#     words=['COLLECT']
#     endorse_ind_search=[([ 'CHANGE', 'FEE'],[]) ]  #list of tuples with must have search and don't have search terms
#     n=12
#     for word in words:
#         endorse_ind_features= ngram_list(n,word, raw_wordlist,endorse_ind_search)

#         features.update(endorse_ind_features)    

    

    return features



def setup_ngram_features( n,who_syn_list, searchlist,raw_wordlist):
    #field_list=[]
    features={}
    for synlist in who_syn_list:
        word=synlist[0]
        for search,not_search in searchlist:
            if not_search:
                for no in not_search:
                    #field_list.append(word +'_X_' +no +'_'+ ('_').join(search))
                    features.update(ngram_list(n,synlist, raw_wordlist,searchlist))
            else:
                   #field_list.append(word +'_' + ('_').join(search)) 
                   features.update(ngram_list(n,synlist, raw_wordlist,searchlist))
    return features




#############  TRAINING  ###########################

import collections
Performance = collections.namedtuple('Performance', ['run', 'category' ,'actual', 'guess'])
Missed = collections.namedtuple('Missed', ['id', 'run',  'category' ,'actual', 'guess'])
error_log_log={}

# Train classifier USING FEATURE LIST from cat16_features

#category='disc_tag1'
#category='pen_curr1'
#category='fee_appl_cd'
#category='endorse_ind'
category='residual_pen'
#

##########################
####  to use  >>  processCAT16_add_BOW_features(db.sample7, db.cat16_features, 'disc_tag1')

from nltk.classify.scikitlearn import SklearnClassifier
from sklearn.naive_bayes import MultinomialNB,BernoulliNB

def processCAT16_train_classifier(run,dx, db,cat16, feature_items_list,category):
    t1=datetime.now()
    training_set=[]
    #cat16_feature_items = [ rec['field'] for rec in  db.cat16_features.find()]
    #cat16_feature_items = list(filter(lambda x: not(is_number(x)), cat16_feature_items)) #remove numbers
    #cat16_feature_items = [ feat['field' ] for feat in features.find({'category': category})]
    # curr_codes=list(currencycode.keys())
    # currency_features = [ feat['field' ] for feat in db.cat16_features.find({'field': { '$in': curr_codes }})]

    # cat16_flags= [ feat['field' ] for feat in db.cat16_features.find({'cat16_flag': True})]
    #cat16_feature_items=cat16_flags + currency_features

    ###########
    print('Training on: ', category)

    total=cat16.find(dx).count()
    cnt=0
    currencycodes=set([x['code'] for x in list(db.currency.find({},{'code':1, '_id':0}))])
    

    for rule in cat16.find(dx):
        #training_set.append((find_fare_features(rule, cat16_feature_items),rule[category]))

        features = generate_features(rule,category,feature_items_list,currencycodes)

        training_set.append((features,rule[category]))
        cnt+=1
        if (cnt % 5000) == 0:
            t3=datetime.now()
            cum=(t3-t1).total_seconds()
            rem=((total-cnt)*cum/cnt)/60
            etf=(t3+timedelta(minutes=rem)).strftime('%H:%M')
            print(cnt, ' faresheets processed')
            stat=f'{cum:.1f} seconds so far. Estimated to complete in {rem:.1f} minutes at {etf}'        
            print(stat)
            
    t4=datetime.now()    
    bench=(t4-t1).total_seconds()/60
    msg=f'TOTAL features setup time: {bench:.1f} minutes'
    print(msg)




    #t=t4.strftime('%Y%m%H%M')
    if category in ['disc_tag1','disc_tag2', 'disctag5', 'disctag8', 'disctag9', 'disctagNOA', 'disctags','disctags_9_NOA', 'tkt_val']:
        BNB_classifier = SklearnClassifier(BernoulliNB())
        BNB_classifier.train(training_set)
        fn="BNB_"+ category + '_'+ str(run) + ".pickle"
        classifier=save_classifiers(fn,BNB_classifier)
    else:
        classifier = nltk.NaiveBayesClassifier.train(training_set)
        fn="naivebayes_"+ category + '_'+ str(run) + ".pickle"  #use run number to tag classifiers
        classifier=save_classifiers(fn,classifier)


    print('Training:', category)
    print("classifier saved as: ", fn)
    t5=datetime.now()   
    bench2=(t5-t4).total_seconds()/60
    msg=f'TOTAL training time: {bench2:.1f} minutes'
    print(msg)
    return (classifier,fn)

def save_classifiers(fn,classifier):    
    save_classifier = open(fn,"wb")
    pickle.dump(classifier, save_classifier)
    save_classifier.close()

def get_classifier(fn):
    classifier_f = open(fn, "rb")
    classifier = pickle.load(classifier_f)
    classifier_f.close()  
    return classifier  

############ Evaluation and error analysis  ##########

def processCAT16_evaluation(run, db, dx,cat16, category, feature_items_list):
    t1=datetime.now()
    ############## TIMING  ##############
    print("Start evaluating classifier for: ", category)
    cnt=Counter()
    performance_cnt=Counter()
    guess_cnt=Counter()
    error_cnt=Counter()
    dev_test_set=[]
    error_log=[]
    #classifier=classifiertup[0]
    currencycodes=set([x['code'] for x in list(db.currency.find({},{'code':1, '_id':0}))])
    run_info=db.runs.find_one({'run':run, 'category':category},
                                sort=[('date', pymongo.DESCENDING)] )

    if not(run_info):
        print ('Bad run number. Unable to find classifiers with run ', run)
        return []
    print('Using classifier:', run_info['classifier' ])
    classifier = get_classifier(run_info['classifier' ])

    #cat16_feature_items = [ feat['field' ] for feat in db.cat16_features.find({'number': False})]
    #cat16_feature_items = [ feat['field' ] for feat in db.cat16_features.find({'field': { '$in': curr_codes }})]
    #category='disc_tag1'  ## get this from the training

    for rule in cat16.find(dx):   #, 'disc_tag1': None
        #features=find_features(rule, cat16_feature_items)
        #features={feat : rule[feat] for feat in features  }
        #features={feat : rule[feat] for feat in feature_items_list  }
        #features.update(get_word_features(rule['l_wordlist']))
        features = generate_features(rule,category,feature_items_list,currencycodes)
        cat= rule[category]
        dev_test_set.append((features,cat))

        guess=classifier.classify(features)
        if is_number(guess):
            guess=float(guess)
        cnt[cat]+=1
        tup=Performance(run= run, category=category, actual=cat, guess=guess)
        performance_cnt[tup]+=1
        if guess != cat:
            #print('features:', features, 'cat:',cat)
            error_log.append(Missed(id=rule['_id'],run= run,category=category,actual=cat, guess=guess))
            error_cnt[tup]+=1 #count which ones we are getting wrong

    error_log_log[category]=error_log #cache the error log for this category
    total_errors = sum(error_cnt.values())
    total_records= sum(cnt.values())
    accuracy=100-(total_errors*100/total_records)
    db.runs.update_one({'run':run, 'category':category,  'classifier': run_info['classifier' ]},{'$set'  :{'accuracy': accuracy}})
    print('Predicting:', category)
    msg=f'Accuracy: {accuracy:.2f} %'
    print(msg)

    ########## summarise errors ###############

    if accuracy<100:
        print('Most common errors:')
        mc=error_cnt.most_common(5)
        list(map(print, mc))


        df = pd.DataFrame.from_dict(error_cnt, orient='index').reset_index()
        df = df.rename(columns={'index':'tup', 0:'count'})
        df.loc[:, 'actual'] =df.apply(lambda row: row['tup'].actual, axis=1)
        df.loc[:, 'guess'] =df.apply(lambda row: row['tup'].guess, axis=1)
        df.loc[:, 'run'] =df.apply(lambda row: row['tup'].run, axis=1)
        df["actual"]=df["actual"].astype('category')
        df["guess"]=df["guess"].astype('category')
        df["run"]=df["run"].astype('category')
        print('Errors')
        error_analysis_df=df.groupby('actual',as_index=False)[['count']].sum()
        error_analysis_df.loc[:, 'run'] =error_analysis_df.apply(lambda row: run, axis=1)
        error_analysis_df.loc[:, 'category'] =error_analysis_df.apply(lambda row: category, axis=1)
        

        #summarise total count
        df = pd.DataFrame.from_dict(performance_cnt, orient='index').reset_index()
        df = df.rename(columns={'index':'tup', 0:'count'})
        df.loc[:, 'category'] =df.apply(lambda row: row['tup'].category, axis=1)
        df.loc[:, 'actual'] =df.apply(lambda row: row['tup'].actual, axis=1)
        df.loc[:, 'guess'] =df.apply(lambda row: row['tup'].guess, axis=1)
        df.loc[:, 'run'] =df.apply(lambda row: row['tup'].run, axis=1)
        df["actual"]=df["actual"].astype('category')
        df["guess"]=df["guess"].astype('category')
        df["run"]=df["run"].astype('category')
        
        actual_analysis_df=df.groupby('actual',as_index=False)[['count']].sum()
        actual_analysis_df.loc[:, 'errors']=error_analysis_df['count']
        actual_analysis_df.loc[:, '% error'] =actual_analysis_df.apply(lambda row: error_pct(row)  , axis=1)
        actual_analysis_df.loc[:, 'run'] =actual_analysis_df.apply(lambda row: run, axis=1)
        actual_analysis_df.loc[:, 'category'] =actual_analysis_df.apply(lambda row: category, axis=1)


        #actual_analysis_df.fillna(" ")

        save_results(db,'labnotes_raw',df)
        save_results(db,'labnotes_error_analysis',   error_analysis_df)
        save_results(db,'labnotes_actual_analysis', actual_analysis_df)
        # t2=datetime.now()
        # bench=(t2-t1).total_seconds()/60
        # db.scorecard.insert_one({   'run': run,
        #                             'targeting_fields' :category,
        #                             'accuracy': accuracy,
        #                             'feature_set': features,
        #                             'classifier':run_info['classifier' ],
        #                             'processing_time':bench})
        ############## TIMING  ############## 


        #return error_log
    else:
        error_log=[]

    t2=datetime.now()
    bench=(t2-t1).total_seconds()/60
    db.scorecard.insert_one({   'run': run,
                                'targeting_fields' :category,
                                'accuracy': accuracy,
                                'feature_set': features,
                                'classifier':run_info['classifier' ],
                                'processing_time':bench}) 


    #run_info=db.scorecard.find_one(sort=[('run', pymongo.DESCENDING)])
    #scores=db.scorecard.find({'run':run_info['run']}) 
    scores=db.scorecard.find({'run':run})
    row_accuracy=1
    for score in scores:
        row_accuracy=row_accuracy*score['accuracy']/100
    row_accuracy=row_accuracy*100
    msg=f'Overall row level accuracy =  {row_accuracy:.2f}%'
    print(msg)    

    msg=f'TOTAL processing time: {bench:.1f} minutes'
    print(msg)
    return error_log

def error_pct(row):
    if row['errors']:
        return row['errors']*100/row['count']
    else:
        return 0

def save_results(db,collection_name,log):
    if not(log.empty):
        return
    data_json = json.loads(log.to_json(orient='records'))
    db_cm = db[collection_name]
    db_cm.insert_many(data_json)


# Backup collection
def backup_collection(db,collection):
    t1=datetime.now()
    t=t1.strftime('%Y%m%H%M')
    pipeline = [ {"$match": {}}, 
             {"$out": collection+t},
                ]
    db_cm=db[collection]
    db_cm.aggregate(pipeline)
    return collection+t


class cat16_rule(object):
    def __init__(self, fare):
        self.disctag9=False
        self.disctag8=False
        self.disctagNOA=False
        self.disctag5=False
        self.disc_tag1=fare['disc_tag1']
        self.disc_tag2=fare['disc_tag2']

    def train(self):
        if self.disc_tag1==9 or self.disc_tag2==9 :
           self.disctag9=True 
        if self.disc_tag1==8 or self.disc_tag2==8 :
           self.disctag8=True 
        if self.disc_tag1=='NOA' or self.disc_tag2=='NOA' :
           self.disctagNOA=True 
        if self.disc_tag1==5 or self.disc_tag2==5 :
           self.disctag5=True 
        return {'disctag9':self.disctag9, 'disctag8': self.disctag8, 'disctagNOA':self.disctagNOA,'disctag5':self.disctag5 }

def get_union_keys(dict_list):
    #returns the union of all keys in the dict list
    tag_list=Counter()
    for el in (map(list, (list(map(lambda x: x.keys( ),dict_list))))):
        tag_list+=Counter(el) 
    return list(tag_list.keys() )


def long_words(text,length):
    txt=[]
    #FARE_STOP_WORDS=['DEST', 'Same', 'Actual']
    for w in text:
        txt+=w.split(' ')#break text into individual words
    return [w for w in txt if len(w)>length]


def update_long_words(fs):
    for record in fs.find({},{'wordlist':1}):
        test=long_words(record['wordlist'],3)
        fs.update_one(record,({'$set':{'long_words':test}}))

def get_field_as_list(collection,field):
    return list(map(lambda x: x[field], list(collection.find({},{field:1, '_id':0}))))
