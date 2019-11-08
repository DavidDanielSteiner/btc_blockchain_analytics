'''
https://www.kaggle.com/wprice/bitcoin-mining-pool-classifier
https://bitcoindev.network/bitcoin-analytics-using-google-bigquery/
https://www.bmc.com/blogs/keras-neural-network-classification/
'''

import tensorflow as tf
from keras.models import Sequential
import pandas as pd
from keras.layers import Dense
import seaborn as sns
import matplotlib as plt
import numpy as np

'''Import and merge Data'''
exchange_labels = pd.read_csv('../data/sample_exchanges_100k.csv', index_col = False)
exchange_features = pd.read_csv('../data/exchange_features.csv', index_col = False)
exchange = exchange_labels.merge(exchange_features, how = "inner", on="address")

other_labels = pd.read_csv('../data/sample_other_100k.csv')
other_features = pd.read_csv('../data/other_features.csv')
other = other_features.merge(other_labels, how = "inner", on="address")

#data = other
data = exchange.append(other)


'''Data Preparation'''
data['first_transaction'] = pd.to_datetime(data['first_transaction'])
data['last_transaction'] = pd.to_datetime(data['last_transaction'])
data['days_old'] = (data['last_transaction'] - data['first_transaction']).dt.days

tmp1 = data[data['type'] == 'sent']
tmp2 = data[data['type'] == 'received']
data = tmp1.merge(tmp2, how = "inner", on="address")


data.rename(columns = {"category_y": "category", 
                       "owner_x":"owner",                                             
                       }, 
                        inplace = True) 

#data = data.merge(wallets, how = "inner", on="address")
#data.drop(['address', 'type_x', 'owner', 'type_y'], axis=1, inplace=True)
#data.to_csv("trainingset_2_200k.csv", index=False)



'''Data Exploration
#data = pd.read_csv('../data/trainingset_1.csv', index_col = False)

data.describe()
data.info()
data.dtypes
data['category'].value_counts()

corr = data.corr()
sns.heatmap(corr, 
            xticklabels=corr.columns.values,
            yticklabels=corr.columns.values)
data['number_transactions_x'].corr( data["days_old_y"])

'''


'''Feature Engineering'''
#data = data[(data['category']=='Gambling') | (data['category']=='Services')]
data.loc[data['category'] != 'Exchange', 'category'] = 0
data.loc[data['category'] == 'Exchange', 'category'] = 1


data['first_transaction_x'] = pd.to_datetime(data['first_transaction_x']).dt.year
data['last_transaction_x'] = pd.to_datetime(data['last_transaction_x']).dt.year
data.drop(['last_transaction_y', 'first_transaction_y', 'address', 'type_x', 'type_y', 'owner', 'owner_y' ,'category_x', 'days_old_y'], axis=1, inplace=True)
#data.drop(['last_transaction_y', 'first_transaction_y', 'address', 'type_x', 'owner', 'type_y'], axis=1, inplace=True)
#data.drop(data.columns[:1], axis=1, inplace=True)

feature_num = len(data.columns)-1

labels=data['category']
features = data.iloc[:,0:feature_num]


'''KERAS'''
from sklearn.model_selection import train_test_split
X=features
y=np.ravel(labels)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.10, random_state=42) 

from sklearn.preprocessing import StandardScaler
scaler = StandardScaler().fit(X_train)
X_train = scaler.transform(X_train)
X_test = scaler.transform(X_test) 

from keras.models import Sequential
from keras.layers import Dense

model = Sequential()
model.add(Dense(feature_num, activation='relu', input_shape=(feature_num,)))
model.add(Dense(feature_num, activation='relu'))
model.add(Dense(1, activation='sigmoid'))

model.compile(loss='binary_crossentropy',
              optimizer='sgd',
              metrics=['accuracy'])
                   
model.fit(X_train, y_train,epochs=5, batch_size=1, verbose=1)


for layer in model.layers:
    weights = layer.get_weights()


y_pred = model.predict_classes(X_test)
score = model.evaluate(X_test, y_test,verbose=1)
print(score)




'''Sklearn'''
data_final_vars=features.columns.values.tolist()
y=['y']
X=[i for i in data_final_vars if i not in y]
from sklearn.feature_selection import RFE
from sklearn.linear_model import LogisticRegression
logreg = LogisticRegression()
rfe = RFE(logreg, 20)
rfe = rfe.fit(features, labels.values.ravel())
print(rfe.support_)
print(rfe.ranking_)

from sklearn.model_selection import train_test_split
X=features
y=np.ravel(labels)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.05, random_state=42) 

logreg = LogisticRegression()
logreg.fit(X_train, y_train)
y_pred = logreg.predict(X_test)
print('Accuracy of logistic regression classifier on test set: {:.2f}'.format(logreg.score(X_test, y_test)))

from sklearn.metrics import confusion_matrix
confusion_matrix = confusion_matrix(y_test, y_pred)
print(confusion_matrix)

from sklearn.metrics import classification_report
print(classification_report(y_test, y_pred))


import statsmodels.api as sm
logit_model=sm.Logit(y_train,X_train)
result=logit_model.fit()
print(result.summary())
