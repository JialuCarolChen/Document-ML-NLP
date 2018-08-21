import string
from string import digits
import jieba
from nltk.corpus import stopwords as sw
from nltk.corpus import wordnet as wn
from nltk import wordpunct_tokenize
from nltk import WordNetLemmatizer
from nltk import sent_tokenize
from nltk import pos_tag
from sklearn.base import BaseEstimator, TransformerMixin
import pymongo
from pymongo import MongoClient
from data.zhtools.langconv import *


class NLTKPreprocessor_EN(BaseEstimator, TransformerMixin):
    """
    an english text string preprocessor to integrate with sklearn vectorizer and classifier
    """

    def __init__(self, stopwords=None, punct=None,
                 lower=True, strip=[]):
        self.lower = lower
        if strip == []:
            self.strip = ['¥', ';¥', '::®/', '%', '&', '*', '@', '-', '_', '//', '///', '//',
                          '#', '$', '£', '^', '(', ')', '*', '{', '}', '•', '••', '·', '）', '）', '\\..®',
                          '(', ')', '[', ']', '!', '?', ';', '.', ':', '，', '。', '%。']
        else:
            self.strip = strip
        # get stopwords
        self.stopwords = stopwords or sw.words('english')
        # add on stopwords:
        self.stopwords = self.stopwords + [
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
            'r', 's', 't', 'u', 'v', 'w', 'x', 'w', 'z'] + [
                             'jun', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'
                         ] + ['mon', 'tues', 'tue', 'wed', 'thurs', 'fri', 'sat', 'sun']

        self.punct = punct or set(string.punctuation)
        self.lemmatizer = WordNetLemmatizer()

    def fit(self, X, y=None):
        return self

    def inverse_transform(self, X):
        return [" ".join(doc) for doc in X]

    def transform(self, X):
        return [
            list(self.tokenize(doc)) for doc in X
        ]

    def tokenize(self, document):
        # Break the document into sentences
        for sent in sent_tokenize(document):
            # Break the sentence into part of speech tagged tokens
            for token, tag in pos_tag(wordpunct_tokenize(sent)):

                # get rid of the number in token
                remove_digits = str.maketrans('', '', digits)
                token = token.translate(remove_digits)

                # Apply preprocessing to the token
                token = token.lower() if self.lower else token

                # strip the token with the strip list
                token = token.strip()
                for s in self.strip:
                    token = token.strip(s)

                    # ignore the token in the strip list
                if token in self.strip:
                    continue

                # If stopword, ignore token and continue
                if token in self.stopwords:
                    continue

                # If punctuation, ignore token and continue
                if all(char in self.punct for char in token):
                    continue

                # Lemmatize the token and yield
                lemma = self.lemmatize(token, tag)
                yield lemma

    def lemmatize(self, token, tag):
        tag = {
            'N': wn.NOUN,
            'V': wn.VERB,
            'R': wn.ADV,
            'J': wn.ADJ
        }.get(tag[0], wn.NOUN)

        return self.lemmatizer.lemmatize(token, tag)


class NLTKPreprocessor_CN(BaseEstimator, TransformerMixin):
    """
    an chinese text string preprocessor to integrate with sklearn vectorizer and classifier
    """

    def __init__(self, stopwords=None, punct=None, strip=[]):

        if strip == []:
            self.strip = ['¥', ';¥', '::®/', '%', '&', '*', '@', '-', '–', '——', '//', '///', '//', '、',
                          '#', '$', '£', '·', '（', '）', '*', '「', '」', '•', '••', '·', '）', '）', '\\..®',
                          '【', '】', '！', '？', '：', '.', '；', '，', '。', '%。', '“']
        else:
            self.strip = strip

        self.punct = punct or set(string.punctuation)

    def fit(self, X, y=None):
        return self

    def inverse_transform(self, X):
        return [" ".join(doc) for doc in X]

    def transform(self, X):
        return [
            list(self.tokenize(doc)) for doc in X
        ]

    def tokenize(self, document):
        seg_list = jieba.cut(document, cut_all=False)
        for token in seg_list:
            # get rid of the number in token
            remove_digits = str.maketrans('', '', digits)
            token = token.translate(remove_digits)

            # strip the token with the strip list
            token = token.strip()
            for s in self.strip:
                token = token.strip(s)

            # ignore the token in the strip list
            if token in self.strip:
                continue
            # If punctuation, ignore token and continue
            if all(char in self.punct for char in token):
                continue

            yield token

    def Traditional2Simplified(self, sentence):
        '''
        A function to convert traditional chinese to simplified chinese
        :param sentence: a traditional chinese string to convert
        :return: converted string
        '''
        sentence = Converter('zh-hans').convert(sentence)
        return sentence

def convert_Traditional2Simplified(db, faresheet):
    """a function to convert the traditional chinese text string stored on MongoDB to simplified chinese text string"""
    fs = db[faresheet]
    ### Find documents with traditional chinese
    Hant_docs = [doc for doc in fs.find({'lang': 'zh-Hant'},
                                        {'filename': 1,'country': 1, 'teststring': 1})]

    ### Convert and update the document language
    cnt = 0
    for doc in Hant_docs:
        traditional_sentence = doc['teststring']
        simplified_sentence = NLTKPreprocessor_CN().Traditional2Simplified(traditional_sentence)
        # check the conversion:
        if len(traditional_sentence) != len(simplified_sentence):
            print("Fail passing conversion check: ", doc['filename'], doc['country'])
        # updating
        fs.update_one(
            {'_id': doc['_id']},
            {"$set": {'lang': 'zh-cn', 'teststring': simplified_sentence}}
        )
        print(cnt)
        cnt += 1






