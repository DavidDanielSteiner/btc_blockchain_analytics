# -*- coding: utf-8 -*-
"""
Created on Tue Dec 17 15:04:53 2019

@author: David
"""

#Random Forest
#https://stackabuse.com/random-forest-algorithm-with-python-and-scikit-learn/
#https://towardsdatascience.com/hyperparameter-tuning-the-random-forest-in-python-using-scikit-learn-28d2aa77dd74

#GridSearch
#https://mlfromscratch.com/gridsearch-keras-sklearn/#/

#Label Encoder
#le = labelencoder.fit_transform(labels)  #https://stackoverflow.com/questions/56266011/how-to-go-back-from-one-hot-encoded-labels-to-single-column-using-sklearn
#tmp = labelencoder.inverse_transform(y_test)

#Pandas
#https://stackoverflow.com/questions/17071871/how-to-select-rows-from-a-dataframe-based-on-column-values

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt   
import xgboost as xgb
import lightgbm as lgb
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, plot_confusion_matrix

# =============================================================================
# 
# =============================================================================

def algorithm_pipeline(X_train_data, X_test_data, y_train_data, y_test_data, 
                       model, param_grid, cv=5, scoring_fit='accuracy',
                       do_probabilities = True, model_evaluation = True, 
                       search_mode = 'GridSearchCV', n_iterations = 10):
    fitted_model = None
    
    if(search_mode == 'GridSearchCV'):
        gs = GridSearchCV(
            estimator=model,
            param_grid=param_grid, 
            cv=cv, 
            n_jobs=-1, 
            scoring=scoring_fit,
            verbose=2
        )
        fitted_model = gs.fit(X_train_data, y_train_data)

    elif (search_mode == 'RandomizedSearchCV'):
        rs = RandomizedSearchCV(
            estimator=model,
            param_distributions=param_grid, 
            cv=cv,
            n_iter=n_iterations,
            n_jobs=-1, 
            scoring=scoring_fit,
            verbose=2
        )
        fitted_model = rs.fit(X_train_data, y_train_data)
    
    print(fitted_model.best_params_)
    
    if(fitted_model != None):
        if do_probabilities:
            pred = fitted_model.predict_proba(X_test_data)
        else:
            pred = fitted_model.predict(X_test_data)
            
            
    if model_evaluation:     
        #Precision-Recall curve
        #ROC
        #https://scikit-learn.org/stable/modules/model_evaluation.html
        y_pred = np.argmax(pred, axis = 1)        
        print(classification_report (y_test, y_pred))

        #Plot Confusion Matrix
        titles_options = [("Confusion matrix, without normalization", None),
                  ("Normalized confusion matrix", 'true')]
        for title, normalize in titles_options:
            disp = plot_confusion_matrix(fitted_model, X_test, y_test,
                                         display_labels=labels,
                                         values_format = '4.2f',
                                         xticks_rotation = 'vertical',
                                         cmap=plt.cm.Blues,
                                         normalize=normalize)
            disp.ax_.set_title(title)
                       
    return fitted_model, pred


# =============================================================================
# Load dataset
# =============================================================================
from sklearn.preprocessing import LabelEncoder
labelencoder = LabelEncoder()

df = pd.read_csv("data/testdata_30k_features.csv")
df = df.drop(['address'], axis = 1)  
df = df.fillna(0)
df['tx_per_day'] = np.where(df['tx_per_day'] == np.inf, df['n_tx'], df['tx_per_day'])
df = df.loc[df['class'].isin(['Exchange','Gambling','Market','Mixer','Pool'])]
#df = df.drop(columns=df.iloc[:,6:15])

#get encoded labels
df['target'] = labelencoder.fit_transform(df['class'])
labels = df.drop_duplicates(subset='class')
labels = (labels.sort_values(by='target')['class']).to_list()
df = df.drop(columns='target')

#features, targets
df['class'] = labelencoder.fit_transform(df['class'])

X = df.loc[:, df.columns != 'class']
y = df['class']

# random oversampling
from imblearn.over_sampling import SMOTE
sm = SMOTE(sampling_strategy='auto')
X, y = sm.fit_resample(X, y)

#test, train split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# =============================================================================
# Logistic Regression
# =============================================================================
model = LogisticRegression()
param_grid = {
        'C':[0.5, 1.0, 2.0], 
        'max_iter':[500, 1000, 5000], 
        'tol':[0.0001, 0.0002],
}

model, pred = algorithm_pipeline(X_train, X_test, y_train, y_test, model, 
                                 param_grid, cv=10, scoring_fit='accuracy',
                                 search_mode = 'RandomizedSearchCV', n_iterations = 2)


# =============================================================================
# Random Forest
# =============================================================================
model = RandomForestClassifier()
param_grid = {
    'n_estimators': [2000, 3000, 5000],
    'max_depth': [18,20,22,24],
    'max_leaf_nodes': [600, 800]
}

model, pred = algorithm_pipeline(X_train, X_test, y_train, y_test, model, 
                                 param_grid, cv=10, scoring_fit='accuracy',
                                 search_mode = 'RandomizedSearchCV', n_iterations = 2)


# =============================================================================
# XBGBoost Regression
# =============================================================================
model = xgb.XGBClassifier()
param_grid = {
    'n_estimators': [400, 700, 1000],
    'colsample_bytree': [0.7, 0.8],
    'max_depth': [15,20,25],
    'reg_alpha': [1.1, 1.2, 1.3],
    'reg_lambda': [1.1, 1.2, 1.3],
    'subsample': [0.7, 0.8, 0.9]
}

model, pred = algorithm_pipeline(X_train, X_test, y_train, y_test, model, 
                                 param_grid, cv=10, scoring_fit='accuracy',
                                 search_mode = 'RandomizedSearchCV', n_iterations = 1)

# =============================================================================
# LightGBM
# =============================================================================
model = lgb.LGBMClassifier()

'''
param_grid = {
    'n_estimators': [400, 700, 1000, 200],
    'colsample_bytree': [0.7, 0.8, 0.9],
    'max_depth': [15,20,25, 40, 60, 100],
    'num_leaves': [50, 100, 200, 500, 1000],
    'reg_alpha': [1.1, 1.2, 1.3],
    'reg_lambda': [1.1, 1.2, 1.3],
    'min_split_gain': [0.3, 0.4],
    'subsample': [0.7, 0.8, 0.9],
    'subsample_freq': [20, 40] 
}
'''

param_grid = {'subsample_freq': [20], 'subsample': [0.7], 'reg_lambda': [1.1], 'reg_alpha': [1.2], 'num_leaves': [200], 'n_estimators': [400], 'min_split_gain': [0.3], 'max_depth': [25], 'colsample_bytree': [0.9]}

model, pred = algorithm_pipeline(X_train, X_test, y_train, y_test, model, 
                                 param_grid, cv=10, scoring_fit='accuracy',
                                 search_mode = 'GridSearchCV', n_iterations = 10)

print(model.best_score_)
print(model.best_params_)








