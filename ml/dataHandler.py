from collections import Counter, defaultdict, deque
from datetime import datetime, date, timedelta
from sklearn.feature_extraction import DictVectorizer
from pymongo import MongoClient
from pymongo import TEXT
import pandas as pd
import os


client = MongoClient('localhost:27017')
db=client.raxdb
collection='CXfaresheets_new'

fs=db[collection]
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

class DataTransfer:

    def __init__(self, db, faresheet):
        self.fs = db[faresheet]
        # default setting for country and list of fields
        self.country = 'ALL'
        self.fields = ['country', 'filename', 'tc_features', 'keyword_features', 'topword_features',
                       'classifications']

    def collect_for_pred(self, lang="en", country = None, fields = None):
        """
        :param classification: the name of the classification task
        :param country: a list of country to include, if country = 'ALL', includes all countries
        :param fields: a list of fields to include, if fields = 'ALL', includes all fields
        :return:
        """
        if country== None:
            country = self.country
        else:
            self.country = country
        if fields == None:
            fields  = self.fields
        else:
            self.fields = fields

        filter = {}
        selection = {}
        if country != 'ALL':
            filter['country'] = {"$in": country}
        if fields != 'ALL':
            for field in fields:
                selection[field] = 1
        filter["lang"] = "en"
        # get document
        docs = [doc for doc in fs.find(filter, selection)]
        print("Collect "+str(len(docs))+" documents")
        return docs


    def collect_for_train(self, classification = None, lang="en", country = None, fields = None):
        """
        :param classification: the name of the classification task
        :param country: a list of country to include, if country = 'ALL', includes all countries
        :param fields: a list of fields to include, if fields = 'ALL', includes all fields
        :return:
        """
        if country== None:
            country = self.country
        else:
            self.country = country
        if fields == None:
            fields  = self.fields
        else:
            self.fields = fields


        # construct the mongo query
        if classification==None:
            filter={}
        else:
            self.classification = classification
            filter = {'classifications.' + classification: {"$exists": True}}
        selection = {}
        if country != 'ALL':
            filter['country'] = {"$in": country}
        if fields != 'ALL':
            for field in fields:
                selection[field] = 1
        filter['lang'] = lang
        # get document
        docs = [doc for doc in fs.find(filter, selection)]
        print("Collect "+str(len(docs))+" documents")
        return docs

    def update_prediction(self, db, faresheet, preds, pred_files_index):
        # update to database
        for i in range(len(preds)):
            if preds[i] == 0:
                pred = "no"
            if preds[i] == 1:
                pred = "yes"
            print("Updating: ", i, pred_files_index[i], pred)
            db[faresheet].update_one({"_id": pred_files_index[i]},
                                     {"$set": {"predictions.Commission": pred}})

    def collect_from_country(self, path, country):
        """a function to collect training data of one market from local csv file"""
        comm = pd.read_csv(path)
        # update classification result
        for index, row in comm.iterrows():
            print(index, row['filename'], row['manual check'])
            fs.update_one({'filename': row['filename'], 'country': country},
                          {'$set': {'classifications.Commission': row['manual check']}})

    def collect_from_multi_country(self, path):
        """a function to collect training data of multiple markets from local csv file"""
        comm = pd.read_csv(path)
        new_cols = []
        for label in comm.columns:
            new_cols.append(label.strip())
        comm.columns = new_cols
        # update classification result
        for index, row in comm.iterrows():
            print(index, row['filename'], row['country'], row['manual check'])
            fs.update_one({'filename': row['filename'], 'country': row['country']},
                          {'$set': {'classifications.Commission': row['manual check']}})


    def collect_from_folder(self, db, faresheet, directory, multi=True):
        """a function to collect training data from a folder of local csv files"""
        if multi:
            for filename in os.listdir(directory):
                if filename.endswith('.csv'):
                    path = directory + '/' + filename
                    print("Collecting from: ", filename)
                    self.collect_from_multi_country(path)
        else:
            for filename in os.listdir(directory):
                if filename.endswith('.csv'):
                    country = '_'.join(filename.split('_')[2:][:-1])
                    path = directory + '/' + filename
                    if country == 'AU_CA_TH':
                        self.collect_from_multi_country(path)
                    else:
                        print("Updating: ", country)
                        self.collect_from_country(path, country)
    # directory = '/Users/chenjialu/Desktop/Commission to load'
    # collect_from_folder(db, faresheet, directory)











    # def check_training_files(db, collection, filename):
    #     file_counter = Counter()
    #     not_found = Counter()
    #     faresheets = db[collection]
    #     list_of_hashes = read_googlesheet(filename, 'Keywords')
    #     for k in list_of_hashes:
    #         # if 'File' not in keyword.keys():
    #         #     continue
    #         keyword = defaultdict(lambda: False, k)
    #         if keyword['File']:
    #             search = faresheets.find_one({'filename': keyword['File'] + '.html'})
    #             if search:
    #                 file_counter[keyword['File']] += 1
    #             else:
    #                 not_found[keyword['File']] += 1
    #
    #     print(len(file_counter.keys()), 'training files in db', len(not_found.keys()), ' are not')
    #     return file_counter, not_found



class DataTransformer:

    def __init__(self):
        # default setting
        self.feature_list = ['tc_features', 'keyword_features', 'topword_features']

    def data_construct(self, target_docs, mode='train', feature_list=None, classification="Commission", labels=["yes", "no"]):
        """
        a function convert mongo classifications documents to data frame

        @param feature_list: a list of classification features to include
        @param docs: mongodb documents which contain the following fields: _id, filename, country and features
        @param labels: a list of valid labelling values
        @param class_n: name of the classification task
        @param mode: either 'training' or 'prediction'

        @return X_train: a list of dictionary, features data
        @return Y_train: a list of labels
        @return files_index: a list of MongoDB document id to identify the files
        """

        if feature_list==None:
            feature_list=self.feature_list
        else:
            self.feature_list=feature_list
        X_dat = []
        Y_dat = []
        files_index = []

        if mode not in ['train', 'predict']:
            print('Invalid mode for this function')
            return None

        for doc in target_docs:
            # check whether is a valid label for training mode
            if mode == 'train':
                valid = False
                for label in labels:
                    if doc['classifications'][classification] == label:
                        valid = True
            else:
                valid = True
            # if it's valid
            if valid:
                features = {}
                for feature in feature_list:
                    dict1 = doc[feature]
                    features.update(dict1)
                    # add features
                X_dat.append(features)
                if mode == 'train':
                    Y_dat.append(doc['classifications'][classification])
                files_index.append(doc['_id'])

        if mode == 'train':
            return X_dat, Y_dat, files_index
        if mode == 'predict':
            return X_dat, files_index

    def train_data_transform(self, X_train, Y_train=None, Y_map=None):
        """
        a function to transform the data to fit into the classifier
        @X_train: a list of dictionaries (features), note that there shouldn't be nan values in the data set
        """
        # transform X
        v = DictVectorizer(sparse=False)
        X_train = v.fit_transform(X_train)
        # transform Y
        if Y_train!=None:
            Y_train = [Y_map[label] for label in Y_train]
            return X_train, Y_train, v.feature_names_, v.vocabulary_
        else:
            return X_train, v.feature_names_, v.vocabulary_

    # X_dat, Y_dat, feature_names, feature_index = train_data_transform(X_dat, Y_dat, Y_map={'yes': 1, 'no': 0})



