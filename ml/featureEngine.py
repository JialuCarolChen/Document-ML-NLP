#Rassure modules
from raxutil.nlp.cat16utilities import *
from raxutil.nlp.production import *
from raxutil.nlp.faresheet import *
from raxutil.nlp.tagger import *
import re

#nltk
from nltk.corpus import stopwords

#to link to Google sheets
import gspread
from oauth2client.service_account import ServiceAccountCredentials

#general
import pandas as pd
from collections import Counter, defaultdict, deque

from datetime import datetime, date, timedelta

import os
from os.path import basename
from glob import glob #for navigating directories
import pickle

#to connect to MongoDB
import pymongo
from pymongo import MongoClient
from pymongo import TEXT

client=MongoClient('mongodb://raxPaper:vN9k32dXma@cluster0-shard-00-00-yvbwp.mongodb.net:27017,cluster0-shard-00-01-yvbwp.mongodb.net:27017,cluster0-shard-00-02-yvbwp.mongodb.net:27017/admin?replicaSet=Cluster0-shard-0&ssl=true&authSource=admin')
#client = MongoClient('localhost:27017')
#client=MongoClient('mongodb://gerald:heroku01@127.0.0.1:27017')
#client=MongoClient('mongodb://raxPaper:vN9k32dXma@127.0.0.1:27017')
db=client.raxdb
filename="00 Fares Keywords List"
# train_dirname='/Users/gt/rassure/Working/CX/Faresheets'
faresheets='CXfaresheets_new'
tourcodes='CXtourcodes'
faresheet_tourcodes='CXfaresheet_tourcodes'
TOPWORDS=''
group='training'
NWORDS=defaultdict(lambda: 1)
#TOURCODE_REGEX=r"[\d|\w]\w+\d+'*,*\s*'*[R*]FF\d+|[\d|\w]\w+\d+'*,*\s*'*[R*]FF\d+"
TOURCODE_REGEX=r"[\d+|\w+]+FF\d+"
tourcodes_master=db['CXtourcodes_master']
tkt_tourcodes=db['CXfaresheet_tourcodes']

#############################################
#           Feature Creation                #
#############################################
def set_features(db, faresheets, group):
    t1=datetime.now()
    fs=db[faresheets]
    df=db['keyword_features']
    cnt=0
    #update_topwords(db,faresheets)


    #feature_list=set_keyword_features(filename)
    feature_list=list(map(lambda x: x['tag'], list(df.find({},{'tag':1,'_id':0}))))

    features={}
    training_set=[]
    category='faretype'
    total=fs.find({group:True}).count()
    print(total,'faresheets to be updated')
    for fare in fs.find({group:True}):
        def_fare=defaultdict(lambda:False,fare)
        for feature in feature_list:
            features[feature]=def_fare[feature]
            features.update(fare['topword_features'])
        fs.update_one({'_id':fare['_id']},{'$set':{'features': features}})
        cnt+=1
        if (cnt % 100) == 0:
            t3=datetime.now()
            cum=(t3-t1).total_seconds()
            rem=((total-cnt)*cum/cnt)/60
            etf=(t3+timedelta(minutes=rem)).strftime('%H:%M')
            print(cnt, ' rules processed')
            stat=f'{cum:.1f} seconds so far. Estimated to complete in {rem:.1f} minutes at {etf}'
            print(stat)
        #training_set.append((features,def_fare[category]))

    #############
    t4=datetime.now()
    bench=(t4-t1).total_seconds()/60
    msg=f'TOTAL processing time: {bench:.1f} minutes'
    print(msg)


def find_adhoc_codes_in_tables(filename, country):
    fare = fs.find_one({'filename': filename, "country": country})
    FARE = FareFile(filename, country)
    tables = FARE.tables()  # <- a lot of heavy swimming is done here
    adhoc_col = ''
    adhoc_codes = []
    new_adhoc = {}
    hdr_row = 0
    TC_fmt = re.compile(TOURCODE_REGEX)
    word_buffer = deque(maxlen=5)
    for table in tables:
        for r, row in enumerate(table):
            for cell in row:
                # print('sec3',k, sec3)
                # print('TEXT',sec3.text)
                word_buffer.append(cell.text.upper())
                if cell.col == adhoc_col and 'ADHOC' not in cell.text.upper():
                    if TC_fmt.search(cell.text):
                        if bool(['BASE' in w for w in word_buffer if 'BASE' in w]):
                            continue
                        else:
                            adhoc_codes += TC_fmt.findall(cell.text)
                            # print(word_buffer)
                            # print(adhoc_col_text, cell.text)

                if 'ADHOC' in cell.text.upper():
                    adhoc_col = cell.col
                    adhoc_col_text = cell.text
                    hdr_row = r
                    # print('ADHOC column:',adhoc_col, adhoc_col_text )

    currAdhoc_codes = fare['adhoc_tourcodes']
    new_adhoc = set(adhoc_codes).difference(set(currAdhoc_codes))
    if new_adhoc:
        print(fare['filename'], 'new adhoc codes:', new_adhoc)
    return new_adhoc


#############################################
#           Feature Encoder                #
#############################################

class TourcodeFeatureEncoder:

    def encode_tourcodes(self, db=db, collection=faresheets):
        """
        A function to create features relating to tourcodes, the tourcodes feature must have already been created
        :param db:
        :param collection:
        :return:
        """
        # add the last digit of any tourcodes in the file name as a feature
        # also same conclusion if there is only one tourcode
        # codes that end in 6 are thought to be adhoc codes
        fs = db[collection]
        fs.update_many({}, {'$set': {'tc_features.tc_lastdigit': False,
                                     'tc_features.tc_adhoc': False}})
        for fare in fs.find({}, {'filename': 1, 'tourcodes': 1}):
            tc_features = self.encode_tourcodes_for_a_fare(fare)
            fs.update_one({'_id': fare['_id']}, {'$set': tc_features})
            print("Updating: ", fare['filename'])

    def encode_tourcodes_for_a_fare(self, fare):
        """
        :param fare:
        :return:
        """
        TC_fmt = re.compile(TOURCODE_REGEX)
        tc_counter = Counter()
        fn = fare['filename']
        tourcodes = fare['tourcodes']
        try:
            adhoc_tourcodes = fare['adhoc_tourcodes']
        except KeyError:
            adhoc_tourcodes = []
        # check_tourcodes=[]
        tc_lastdigit = None
        tc_adhoc = None
        tc_6 = 0  # count of tourcodes ending in 6
        for tourcode in tourcodes:
            tc_lastdigit = tourcode[-1]  # get the last digit of the TourCode
            if tc_lastdigit == '6':
                tc_adhoc = True
                # tc_counter[tc_lastdigit]+=1
            # check_tourcodes+=[tourcode]
        tc_majority = False
        if len(tourcodes) > 0:
            tc_majority = 'ADHOC' if len(adhoc_tourcodes) / len(tourcodes) > 0.49 else 'FILED'
        return {'tc_features.tc_lastdigit': tc_lastdigit,
                'tc_features.tc_adhoc': tc_adhoc,
                'tc_features.tc_majority': tc_majority
                }

        # fare = fs.find_one({'tourcodes.1': {'$exists': True}}, {'filename':1, 'tourcodes':1 })
        # print(encode_tourcodes_for_a_fare(fare))
        # encode_tourcodes(db, collection)

    def encode_tc_firstdigit_is_num(self, db, faresheet, search_dict={}):
        """
        This function create feature tc_features.tc_firstdigit_isNum.
        If all the tourcodes start with number, then tc_features.tc_firstdigit_isNum is updated as true"""
        fs = db[faresheet]
        docs = [doc for doc in fs.find(search_dict, {'tourcodes': 1})]
        cnt = 0
        for doc in docs:
            # check whether the first digit is numerical
            tourcodes = doc['tourcodes']
            tourcodes_fd = [t[0] for t in tourcodes]
            tourcodes_fd_num = []
            for t in tourcodes_fd:
                try:
                    tourcodes_fd_num.append(int(t))
                except ValueError:
                    pass
            if len(tourcodes_fd_num) == len(tourcodes_fd):
                fd_is_num = True
            else:
                fd_is_num = False
            # record it to the collection
            fs.update_one({'_id': doc['_id']},
                          {'$set': {'tc_features.tc_firstdigit_isNum': fd_is_num}})
            cnt = cnt + 1
            print("Updated: ", cnt)

    def encode_tc_has_BCODE(self, db, faresheet, search_dict={}):
        """
        This function create feature tc_features.tc_hasBCODE
        If any of the tourcodes has BCODE, then tc_features.tc_hasBCODE is updated as true
        """
        fs = db[faresheet]
        docs = [doc for doc in fs.find(search_dict, {'tourcodes': 1})]
        cnt = 0
        for doc in docs:
            # check whether the first digit is numerical
            tourcodes = doc['tourcodes']
            tc_hasBCODE = [tc for tc in tourcodes if 'BCODE' in tc]
            if len(tc_hasBCODE) > 0:
                hasBCODE = True
            else:
                hasBCODE = False
                # record it to the collection
            fs.update_one({'_id': doc['_id']},
                          {'$set': {'tc_features.tc_hasBCODE': hasBCODE}})
            cnt = cnt + 1
            print("Updated: ", cnt)



    def set_ATPCO_tourcodes(db, faresheets, tourcodes):
        """
        :param db:
        :param faresheets:
        :param tourcodes:
        :return:
        """
        # set up tourcodes
        tc = db[tourcodes]
        fs = db[faresheets]
        fs.update_many({}, {'$set': {'CAT35_filed': False}})  # set defaults
        # fs.update_many({},{'$unset':{
        #                                 'filed':0,
        #                                 'faretype':0,
        #                                 'training':0}})
        for rec in tc.find({'filed': 'Y'}):
            tourcode = rec['tour_cd']
            # txt=' '.join(keyword['Keywords'].split())
            # tag='_'.join([x.upper() for x in txt.split()])
            # search_txt="\"" + tourcode + "\""
            # feats=fs.find({ "$text": { "$search": search_txt } })
            # found=fs.find({ "$text": { "$search": search_txt }} ,{'_id':1})
            found = fs.find({'tourcodes': {'$in': [tourcode]}}, {'_id': 1})
            if rec['filed'] == 'Y':
                if list(found):
                    # print(list(found), 'tourcode:',tourcode, found.count(), 'files')
                    fs.update_many({'tourcodes': {'$in': [tourcode]}},
                                   {"$addToSet": {"ATPCO_tourcodes": tourcode}})
                    tc.update_one(rec, {'$set': {'no_of_faresheets': found.count()}})
                    fs.update_many({'tourcodes': {'$in': [tourcode]}},
                                   {'$set': {'CAT35_filed': True,
                                             'faretype': 'filed'
                                             }})
        # get correct faretype for paper tourcodes
        found = db.CXfaresheets.find({'tc_features.tc_lastdigit': '6', 'CAT35_filed': True})
        db.CXfaresheets.update_many({'tc_features.tc_lastdigit': '6', 'CAT35_filed': True},
                                    {'$set': {'faretype': 'paper'}})
        # else:
        #     #check if it is a paperfare

        #     fs.update_many({ "$text": { "$search": search_txt } },
        #               { "$addToSet": { "tourcodes": tourcode , 'filed':False} })
        # print(tourcode, feats.count())

class AdhocFeatureEncoder:

    def encode_adhoc_features(db, collection, search_dict={}):  # expert opinion
        # Consolidate 'adhoc' keywords 26/3/18
        # Requires keyword_features to be in place

        adhoc = re.compile(r'ADHOC')
        adhoc_features = {}
        adhoc_features['adhoc_features'] = False
        fs = db[collection]

        search_dict['keyword_features'] = None
        missing_kw = fs.find(search_dict).count()
        if missing_kw:
            print(missing_kw, 'files are missing keyword_features')
            print('Encoding stopped')
            return
        search_dict['keyword_features'] = {'$exists': True}
        # fs.update_many({search_dict},{'$unset':{'adhoc_features':0}})

        for fare in fs.find(search_dict):
            # tc_adhoc=fare['tc_features.tc_adhoc']
            # tourcodes=fare['tourcodes']
            # tw=fare['keyword_features'].keys()
            # tw=[adhoc.search(w.upper()).group(0)  for w in tw if adhoc.search(w.upper())]
            # if tw:
            #     adhoc_features['adhoc_features']=True
            #     fs.update_one(fare, {'$set':{'adhoc_features':adhoc_features}}) #dict one level down so it can be used directly as featuresets
            tw = []
            for tag, present in fare['keyword_features'].items():
                if present:
                    try:
                        tw += [adhoc.search(tag).group(0)]
                    except AttributeError:
                        continue
            if tw:
                adhoc_features['adhoc_features'] = True
                fs.update_one({'_id': fare['_id']}, {'$set': {
                    'adhoc_features': adhoc_features}})  # dict one level down so it can be used directly as featuresets




class TopwordFeatureEncoder:

    def update_topwords(db, faresheets, feature='topwords', search_dict={}):
        wordtext = []
        word_counter = Counter()
        fs = db[faresheets]
        dw = db[feature]
        ddmon = re.compile('\d+[JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC]')
        # {'wordlist_check':{'$ne': True}}
        print(fs.find(search_dict).count(), 'files to be updated')

        for fare in fs.find(search_dict, {'wordlist': 1}):
            wordlist = fare['wordlist']
            for word in wordlist:
                # if len(word)>3: #exclude 3 letter words eg currency
                word_counter[word] += 1

        # for record in fs.find({},{'wordlist':1}):
        #     words= record['wordlist']
        #     wordtext += words
        # long_words=[w for w in wordtext if len(w)>3] #exclude 3 letter words eg currency
        stoplist = set(stopwords.words("english"))
        stoplist.discard('no')  # remove 'no' from stop list
        stoplist.discard('not')  # remove 'no' from stop list
        stoplist.discard('before')
        stoplist.discard('after')
        STOPLIST = [x.upper() for x in stoplist]
        STOPLIST += ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY',
                     'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN', ]
        topwords = word_counter.most_common(4000)
        ddmon = re.compile(r'\d+[JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC]')
        nums = re.compile(r'\b\d+[\s]+|\d+')
        TC_fmt1 = re.compile(TOURCODE_REGEX)
        for word, cnt in topwords:
            if len(word) < 4:  # exclude 3 letter words eg currency
                continue
            if word in STOPLIST:
                continue
            if ddmon.search(word):  # exclude dates
                continue
            if TC_fmt1.search(word):  # exclude tourcodes
                continue
            if nums.match(word):  # exclude combinations of numbers and spaces
                continue

            dw.insert_one({'word': word, 'count': cnt})

        TOPWORDS = list(map(lambda x: x['word'], list(dw.find({}, {'word': 1, '_id': 0}))))
        cnt = 0
        fs.update_many(search_dict, {'$set': {feature + '_features': []}})
        for fare in fs.find(search_dict, {'wordlist': 1}):
            topwords_features = find_features(fare['wordlist'], TOPWORDS)
            fs.update_one({'_id': fare['_id']}, {'$set': {feature + '_features': topwords_features}})
            cnt += 1
        print(feature, 'updated.', cnt, 'files updated', len(TOPWORDS), 'words')
        return TOPWORDS


class KeywordFeatureEncoder:

    def encode_adhoc_form_feature(self, db, faresheet, search_dict={}):
        """
        If it's an ONE-OFF ADHOC FIT/GROUP REQUEST form, it's not commission and not filed.
        :param db:
        :param faresheet:
        :param search_dict:
        :return:
        """
        docs = [doc for doc in db[faresheet].find(search_dict, {"filename": 1, "country": 1, "teststring": 1})]
        docs_df = pd.DataFrame(docs)
        mask = docs_df['teststring'].str.contains("ONE-OFF ADHOC FIT/GROUP REQUEST")
        form_df = docs_df.loc[mask]
        # create keyword feature and update classification result for one-off adhoc fit/group request
        for index, row in form_df.iterrows():
            print("Updating: ", row["filename"], row["country"])
            db[faresheet].update_one({"_id": row['_id']}, {"$set": {"keyword_features.one_off_adhoc_fg_request": True}})
            db[faresheet].update_one({"_id": row['_id']}, {"$set": {"classifications.Commission": "no"}})
            db[faresheet].update_one({"_id": row['_id']}, {"$set": {"classifications.Filed": "no"}})




class OtherFeatureEncoder:

    def encode_cluster_feature(self, db, faresheet, analyzer, dat, k, v_min_df):
        # encode cluster features
        dat = analyzer.find_clusters(dat, k, v_min_df)
        # update the cluster feature to database
        print("Number of files to update: ", len(dat))
        cnt = 0
        for index, row in dat.iterrows():
            print("Updating: ", cnt, row["filename"], row["country"])
            db[faresheet].update_one({
                "_id": row["_id"]
            },
                {"$set": {"other_features.cluster": "US_" + str(row["cluster"])}}
            )
            cnt += 1























































