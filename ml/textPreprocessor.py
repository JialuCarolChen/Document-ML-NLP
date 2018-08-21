import string
from string import digits
from nltk.corpus import stopwords as sw
from nltk.corpus import wordnet as wn
from nltk import wordpunct_tokenize
from nltk import WordNetLemmatizer
from nltk import sent_tokenize
from nltk import pos_tag
from sklearn.base import BaseEstimator, TransformerMixin














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


