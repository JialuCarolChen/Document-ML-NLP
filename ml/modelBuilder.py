import datetime
from nltk.classify.scikitlearn import SklearnClassifier
from nltk import NaiveBayesClassifier
from sklearn.naive_bayes import BernoulliNB
import numpy as np
from sklearn.metrics import classification_report


# def farefiler_train_classifier(run, dx, db, cat16, feature_items_list, category):
#     t1 = datetime.now()
#     training_set = []
#     # cat16_feature_items = [ rec['field'] for rec in  db.cat16_features.find()]
#     # cat16_feature_items = list(filter(lambda x: not(is_number(x)), cat16_feature_items)) #remove numbers
#     # cat16_feature_items = [ feat['field' ] for feat in features.find({'category': category})]
#     # curr_codes=list(currencycode.keys())
#     # currency_features = [ feat['field' ] for feat in db.cat16_features.find({'field': { '$in': curr_codes }})]
#
#     # cat16_flags= [ feat['field' ] for feat in db.cat16_features.find({'cat16_flag': True})]
#     # cat16_feature_items=cat16_flags + currency_features
#
#     ###########
#     print('Training on: ', category)
#
#     total = cat16.find(dx).count()
#     cnt = 0
#     currencycodes = set([x['code'] for x in list(db.currency.find({}, {'code': 1, '_id': 0}))])
#
#     for rule in cat16.find(dx):
#         # training_set.append((find_fare_features(rule, cat16_feature_items),rule[category]))
#
#         features = generate_features(rule, category, feature_items_list, currencycodes)
#
#         training_set.append((features, rule[category]))
#         cnt += 1
#         if (cnt % 5000) == 0:
#             t3 = datetime.now()
#             cum = (t3 - t1).total_seconds()
#             rem = ((total - cnt) * cum / cnt) / 60
#             etf = (t3 + timedelta(minutes=rem)).strftime('%H:%M')
#             print(cnt, ' faresheets processed')
#             stat = f'{cum:.1f} seconds so far. Estimated to complete in {rem:.1f} minutes at {etf}'
#             print(stat)
#
#     t4 = datetime.now()
#     bench = (t4 - t1).total_seconds() / 60
#     msg = f'TOTAL features setup time: {bench:.1f} minutes'
#     print(msg)
#
#     # t=t4.strftime('%Y%m%H%M')
#     if category in ['disc_tag1', 'disc_tag2', 'disctag5', 'disctag8', 'disctag9', 'disctagNOA', 'disctags',
#                     'disctags_9_NOA', 'tkt_val']:
#         BNB_classifier = SklearnClassifier(BernoulliNB())
#         BNB_classifier.train(training_set)
#         fn = "BNB_" + category + '_' + str(run) + ".pickle"
#         classifier = save_classifiers(fn, BNB_classifier)
#     else:
#         classifier = NaiveBayesClassifier.train(training_set)
#         fn = "naivebayes_" + category + '_' + str(run) + ".pickle"  # use run number to tag classifiers
#         classifier = save_classifiers(fn, classifier)
#
#     print('Training:', category)
#     print("classifier saved as: ", fn)
#     t5 = datetime.now()
#     bench2 = (t5 - t4).total_seconds() / 60
#     msg = f'TOTAL training time: {bench2:.1f} minutes'
#     print(msg)
#     return (classifier, fn)

###########################
#    Classification      #
##########################

from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import LeaveOneOut
def train_with_loo(X_dat, Y_dat, clf, class_names=['commission', 'not commission']):
    """
    A function to train with leave-one-out cross validation
    :param X_dat:
    :param Y_dat:
    :param Clf:
    :param rs:
    :return:
    """
    loo = LeaveOneOut()
    golds = []
    preds = []
    for train_index, test_index in loo.split(X_dat):
        # print("leave Out:", test_index[0])
        X_train, X_test = X_dat[train_index], X_dat[test_index]
        y_train = [Y_dat[i] for i in train_index]
        y_test = Y_dat[test_index[0]]
        clf.fit(X_train, y_train)
        pred = clf.predict(X_test)
        preds.append(pred[0])
        golds.append(y_test)
    print(classification_report(golds, preds, target_names=class_names))
    return golds, preds

# get wrongly classified files
def get_wrong_files(golds, preds, files_index):
    wrong_index = [(i, golds[i], preds[i]) for i in range(len(golds)) if golds[i]!=preds[i]]
    wrong_files = [files_index[wrong_index[i][0]] for i in range(len(wrong_index))]
    print("Number of wrongly classified files:", len(wrong_index))
    for i in range(len(wrong_index)):
        print("Wrongly classified file: ", wrong_files[i])
        print("The true label is: ", wrong_index[i][1], "Predicted as: ", wrong_index[i][2])
    return wrong_index, wrong_files

# train with feature importance
def train_with_feature_importances(X_dat, Y_dat, clf, fim_map, feature_index, X_valid=None, Y_valid=None, loo=True, threshold = 0):
    # transform data set
    important_features = [f for f in fim_map if f[1] > threshold]
    important_features_index = [feature_index[f[0]] for f in important_features]
    X_dat_if = [np.take(row, important_features_index) for row in X_dat]
    X_dat_if = np.array(X_dat_if)
    print("Transform data has shape: ",     X_dat_if.shape)
    # if train with loo
    if loo == True:
        golds, preds = train_with_loo(X_dat_if, Y_dat, clf)

    else:
        X_valid_if = [np.take(row, important_features_index) for row in X_valid]
        X_valid_if = np.array(X_valid_if)
        print("Transform data has shape: ", X_dat_if.shape)
        clf.fit(X_dat_if, Y_dat)
        preds = clf.predict(X_valid_if)
        golds = Y_valid
    return golds, preds

#############################################
#     Feature Extraction from classifier     #
#############################################

def get_NB_feature_importances(clf_nb, feature_names):
    fim_maps=[]
    for j in range(len(clf_nb.feature_log_prob_)):
        fim_map = {}
        for i in range(len(feature_names)):
            fim_map[feature_names[i]] = clf_nb.feature_log_prob_[j][i]
        fim_map = sorted(fim_map.items(), key=lambda x: x[1], reverse=True)
        fim_maps.append(fim_map)
    return fim_maps

# get feature importances from Decision Tree
def get_feature_importances(clf1, feature_names):
    fim_map = {}
    for i in range(len(feature_names)):
        fim_map[feature_names[i]] = clf1.feature_importances_[i]

    fim_map = sorted(fim_map.items(), key=lambda x: x[1], reverse=True)
    return fim_map

#############################################
#     Ensemble models                       #
#############################################
# def ensemble_models(clfs, X_dat, Y_dat, feature_select=True):
#     for clf in clfs:
#         clf.fit(X_dat, Y_dat)
#         # get feature importances
#         fim_map = get_feature_importances(clf, feature_names)
#         # transform data set
#         important_features = [f for f in fim_map if f[1] > threshold]
#         important_features_index = [feature_index[f[0]] for f in important_features]
#         X_dat_if = [np.take(row, important_features_index) for row in X_dat]
#         X_dat_if = np.array(X_dat_if)
#         # train with important features
#         clf.fit(X_dat_if, Y_dat)

#############################################
#          Predictions                      #
#############################################

# train with feature importance
def predict_with_feature_importances(X_dat, clf, fim_map, feature_index, threshold = 0):
    # transform data set
    important_features = [f for f in fim_map if f[1] > threshold]
    important_features_index = [feature_index[f[0]] for f in important_features]
    X_dat_if = [np.take(row, important_features_index) for row in X_dat]
    X_dat_if = np.array(X_dat_if)
    print("Transform data has shape: ",     X_dat_if.shape)
    preds = clf.predict(X_dat_if)
    return preds







