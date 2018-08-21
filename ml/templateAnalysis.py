##########################################################
#      Cluster Analysis for Template Identification     #
#########################################################
from sklearn.metrics import silhouette_score
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans

class ClusterAnalyzer :

    def choose_k(self, dat, k1=2, k2=11, v_min_df=2):
        """A function to choose k with tfidf, A higher Silhouette Coefficient indicates that the object is well matched to
        its own cluster and poorly matched to neighboring clusters."""
        tfidf_vectorizer = TfidfVectorizer(min_df=v_min_df, lowercase=False,
                                           stop_words='english', analyzer='word')
        tfidf = tfidf_vectorizer.fit_transform(dat['teststring'])
        self.tfidf_feature_list = tfidf_vectorizer.get_feature_names()
        print("Number of features: ", len(self.tfidf_feature_list))
        for n_cluster in range(k1, k2):
            kmeans = KMeans(n_clusters=n_cluster).fit(tfidf)
            label = kmeans.labels_
            sil_coeff = silhouette_score(tfidf, label, metric='euclidean')
            print("For n_clusters={}, The Silhouette Coefficient is {}".format(n_cluster, sil_coeff))

    def find_clusters(self, dat, k=2, v_min_df=2):
        """
        :param dat:
        :param k:
        :param v_min_df:
        :return:
        """
        tfidf_vectorizer = TfidfVectorizer(min_df=v_min_df, lowercase=False,
                                           stop_words='english', analyzer='word')
        tfidf = tfidf_vectorizer.fit_transform(dat['teststring'])
        self.tfidf_feature_list = tfidf_vectorizer.get_feature_names()
        print("Number of features: ", len(self.tfidf_feature_list))
        self.km_model = KMeans(n_clusters=k)
        self.km_model.fit(tfidf)
        dat['cluster'] = self.km_model.labels_
        dat.sort_values(['cluster', 'filename'], inplace=True)
        return dat

##########################################################
#   Commission Identification from train/predict data   #
#########################################################
import pandas as pd
def get_market_comm(db, faresheet, country=None, docs=None):
    """
    A function to get identified/classified commission sheets
    :param db:
    :param faresheet:
    :param country:
    :return:
    """
    if docs==None:
        us_docs = [doc for doc in db[faresheet].find({'country': country},
                                                 {'predictions': 1, 'classifications': 1, 'path': 1, 'teststring': 1,
                                                  'filename': 1, 'country': 1})]
    else:
        us_docs = docs
    us_df = pd.DataFrame(us_docs)
    us_df['gold'] = ''
    # labelling
    for index, row in us_df.iterrows():
        try:
            us_df.loc[index, 'gold'] = row['classifications']['Commission']
        except:
            print("Can't get commission gold: ", row['filename'])
        us_df.loc[index, 'pred'] = row['predictions']['Commission']

    # get all the predicted/classified commission sheet
    mask1 = us_df['gold'] == 'yes'
    mask2 = us_df['pred'] == 'yes'
    us_comm = us_df.loc[mask1 | mask2]
    # the classified result can't be no
    mask3 = us_df['gold'] != 'no'
    us_comm = us_comm.loc[mask3]
    us_comm.drop(['classifications', 'predictions'], axis=1, inplace=True)

    return us_comm
