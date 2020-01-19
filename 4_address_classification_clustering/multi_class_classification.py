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
import xgboost as xgb
import lightgbm as lgb
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, plot_confusion_matrix

from classification_pipeline import algorithm_pipeline

# =============================================================================
# Load dataset
# =============================================================================
data = pd.read_csv("../data/features_trainingset_all_categories_cleaned.csv")
data = data.drop(['address', 'is_coinbase'], axis = 1)  
df = data.fillna(0)

#df.loc[df.category == 'Mixer', 'category'] = 'Not_Exchange'
#df.loc[df.category == 'Service', 'category'] = 'Not_Exchange'
#df.loc[df.category == 'Mining', 'category'] = 'Not_Exchange'
#df.loc[df.category == 'Gambling', 'category'] = 'Not_Exchange'
#df['tx_per_day'] = np.where(df['tx_per_day'] == np.inf, df['n_tx'], df['tx_per_day'])
#df = df.loc[df['class'].isin(['Exchange','Gambling','Market','Mixer','Pool'])]
#df = df.drop(columns=df.iloc[:,6:15])


#get encoded labels
from sklearn.preprocessing import LabelEncoder
labelencoder = LabelEncoder()
df['target'] = labelencoder.fit_transform(df['category'])
labels = df.drop_duplicates(subset='category')
labels = (labels.sort_values(by='target')['category']).to_list()
df = df.drop(columns='target')

#features, targets
df['category'] = labelencoder.fit_transform(df['category'])

X = df.loc[:, df.columns != 'category']
y = df['category']


'''
#Scaling
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler().fit(X)
X = scaler.transform(X)

# random oversampling all data
from imblearn.over_sampling import SMOTE
sm = SMOTE(sampling_strategy='auto')
X, y = sm.fit_resample(X, y)
'''

#test, train split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


# =============================================================================
# Logistic Regression
#
#Best Params: {'tol': 0.0001, 'max_iter': 5000, 'C': 1.0}
#Accuracy: 0.48 
#F1 macro: 0.42
# =============================================================================
model = LogisticRegression()
param_grid = {
        'C':[0.5, 1.0, 2.0], 
        'max_iter':[1000, 5000, 8000], 
        'tol':[0.0001, 0.0002],
}

model, pred = algorithm_pipeline(X_train, X_test, y_train, y_test, model, 
                                 param_grid, cv=10, scoring_fit='accuracy',
                                 search_mode = 'RandomizedSearchCV', n_iterations = 10,
                                 labels=labels)
# =============================================================================
# Random Forest
#Params: 
#Accuracy: 
#F1 macro: 
# =============================================================================
model = RandomForestClassifier()
param_grid = {
    'n_estimators': [2000, 3000, 5000],
    'max_depth': [18,20,22,24],
    'max_leaf_nodes': [600, 800]
}

model, pred = algorithm_pipeline(X_train, X_test, y_train, y_test, model, 
                                 param_grid, cv=10, scoring_fit='accuracy',
                                 search_mode = 'RandomizedSearchCV', n_iterations = 10,
                                 labels=labels)

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
                                 search_mode = 'RandomizedSearchCV', n_iterations = 10,
                                 labels=labels)


# =============================================================================
# LightGBM
# =============================================================================
model = lgb.LGBMClassifier()

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


#param_grid = {'subsample_freq': [20], 'subsample': [0.7], 'reg_lambda': [1.1], 'reg_alpha': [1.2], 'num_leaves': [200], 'n_estimators': [400], 'min_split_gain': [0.3], 'max_depth': [25], 'colsample_bytree': [0.9]}

model, pred = algorithm_pipeline(X_train, X_test, y_train, y_test, model, 
                                 param_grid, cv=10, scoring_fit='accuracy',
                                 search_mode = 'GridSearchCV', n_iterations = 10,
                                 labels=labels)

#importances = model.best_estimator_.feature_importances_
feature_importances = pd.DataFrame(model.best_estimator_.feature_importances_,
                                   index = X_train.columns,
                                   columns=['importance']).sort_values('importance', ascending=False)


# =============================================================================
# Neural Network v1
# =============================================================================
import pandas
from keras.models import Sequential
from keras.layers import Dense
from keras.wrappers.scikit_learn import KerasClassifier
from keras.utils import np_utils
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
from sklearn.preprocessing import LabelEncoder
from sklearn.pipeline import Pipeline

'''
# define baseline model
def baseline_model():
	# create model
	model = Sequential()
	model.add(Dense(40, input_dim=40, activation='relu'))
    model.add(Dense(40, activation='relu'))
	model.add(Dense(5, activation='softmax'))
	# Compile model
	model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
	return model
 
estimator = KerasClassifier(build_fn=baseline_model, epochs=200, batch_size=5, verbose=0)
kfold = KFold(n_splits=10, shuffle=True)
results = cross_val_score(estimator, X_train, y_train, cv=kfold)
print("Baseline: %.2f%% (%.2f%%)" % (results.mean()*100, results.std()*100))
'''

# =============================================================================
# Neural Network v2
# =============================================================================
df = data.fillna(0)

#feature and target split
X = df.loc[:, df.columns != 'category']
y = df['category']

#Standartization
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler().fit(X)
X = scaler.transform(X)

#Label encoding
from sklearn.preprocessing import LabelBinarizer
encoder = LabelBinarizer()
y = encoder.fit_transform(y)

#test, train split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

'''
# random oversampling training data
from imblearn.over_sampling import SMOTE
sm = SMOTE(sampling_strategy='auto')
X_train, y_train = sm.fit_resample(X_train, y_train)
'''


from keras.models import Sequential
from keras.layers import Dense, Dropout

# Initialize the constructor
model = Sequential()
# input layer 
model.add(Dense(40, input_shape=(40,), activation='relu'))
# hidden layer 
model.add(Dropout(0.2))
model.add(Dense(40, input_shape=(40,),activation='relu'))
model.add(Dropout(0.2))
model.add(Dense(40, input_shape=(40,),activation='relu'))
# output layer 
model.add(Dense(5, input_shape=(40,), activation='softmax'))

#Model details
model.output_shape
model.summary()
model.get_config()
model.get_weights()

model.compile(loss='categorical_crossentropy', #categorical_crossentropy, binary_crossentropy
              optimizer='adam', #SGD, RMSprop, adam
              metrics=['accuracy'])
                   
model.fit(X_train, y_train,epochs=100, batch_size=10, verbose=1)

#Evaluation
pred = model.predict(X_test)
score = model.evaluate(X_test, y_test,verbose=1)

y_pred = np.argmax(pred, axis = 1)
y_test = np.argmax(y_test, axis = 1)  

# Import the modules from `sklearn.metrics`
from sklearn.metrics import confusion_matrix, precision_score, recall_score, f1_score, cohen_kappa_score

# Confusion matrix
confusion_matrix(y_test, y_pred)
precision_score(y_test, y_pred, average='micro')
recall_score(y_test, y_pred, average='micro')
f1_score(y_test,y_pred, average='micro')
f1_score(y_test,y_pred, average='macro')
f1_score(y_test,y_pred, average='weighted')
print(classification_report (y_test, y_pred))



# =============================================================================
# Save model
# =============================================================================
from sklearn.externals import joblib

filename = 'model.sav'
joblib.dump(model, '../models/' + filename)


# =============================================================================
# Predict unknown transactions
# =============================================================================
loaded_model = joblib.load(filename)

df = pd.read_csv("../data/features_unknown.csv")
address = df['address']
df = df.drop(['address'], axis = 1)  
df = df.drop(['mean_value_percent_marketcap', 'std_value_percent_marketcap', 'mean_tx_value_percent_marketcap', 'std_tx_value_percent_marketcap'], axis = 1)  
df = df.fillna(0)
unknown = df


pred = list(loaded_model.predict(unknown))
df['category'] = pred
df['address'] = address
df['owner'] = 'predicted'
 

for category_number, category_name in enumerate(labels):
    df.loc[df.category == category_number, 'category'] = category_name


predicted_wallets = df[['address', 'owner', 'category']]
predicted_wallets.to_csv("addresses_predicted.csv", index=False)

##############
wallet_owners_2 = predicted_wallets.groupby(['owner', 'category']).agg(['count'], as_index=False).reset_index()

df = pd.read_csv("../data/final_dataset/addresses_known_0.01_marketcap_2015.csv")
wallet_owners_3 = df.groupby(['category']).agg(['count'], as_index=False).reset_index()


