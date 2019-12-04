# -*- coding: utf-8 -*-
"""
Created on Wed Nov 27 14:53:06 2019

@author: David

https://towardsdatascience.com/outlier-detection-with-one-class-svms-5403a1a1878c
https://www.datacamp.com/community/tutorials/understanding-logistic-regression-python
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression #https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html

df = pd.read_csv("data/cluster_data.csv", index_col=False)
df.loc[df.category == 'Exchange', 'exchange'] = 1
df.loc[df.category != 'Exchange', 'exchange'] = 0
df = df[['txns', 'adr_per_tnx', 'dollar_mean', 'dollar_median', 'dollar_max', 'exchange']]

data = df.values
X = data[:,0:5]
X = (X - np.mean(X)) / np.std(X)
Y = data[:,5]
#log_data = np.log(df)

X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.33, random_state=42)

features = df[['txns', 'adr_per_tnx', 'dollar_mean', 'dollar_median', 'dollar_max']]
label = df[['exchange']]
stan_features = (features-features.mean())/features.std()
norm_features =(features-features.min())/(features.max()-features.min())
df_stan = stan_features.join(label)
df_norm = norm_features.join(label)

#undersampling
from imblearn.under_sampling import RandomUnderSampler 
rus = RandomUnderSampler() 
X_resampled, y_resampled = rus.fit_sample(X_train, y_train) 
clf = LogisticRegression() 
clf.fit(X_resampled, y_resampled)

#oversampling
from imblearn.over_sampling import ADASYN 
ada = ADASYN() 
X_resampled, y_resampled = ada.fit_sample(X_train, y_train) 
clf = LogisticRegression() 
clf.fit(X_resampled, y_resampled)

#under and oversampling
from imblearn.combine import SMOTEENN 
smo = SMOTEENN() 
X_resampled, y_resampled = smo.fit_sample(X_train, y_train) 
clf = LogisticRegression() 
clf.fit(X_resampled, y_resampled)

'''One class SVM'''
from sklearn.svm import OneClassSVM 
train, test = train_test_split(df_stan, test_size=.2) 
train_normal = train[train['exchange']==0] 
train_outliers = train[train['exchange']==1] 
#outlier_prop = 0.5
outlier_prop = len(train_outliers) / len(train_normal) 
svm = OneClassSVM(kernel='rbf', nu=outlier_prop, gamma=0.1) 
svm.fit(train_normal[['txns', 'adr_per_tnx', 'dollar_mean', 'dollar_median', 'dollar_max']])

x = test['dollar_max'] 
y = test['adr_per_tnx'] 
y_pred = svm.predict(test[['txns', 'adr_per_tnx', 'dollar_mean', 'dollar_median', 'dollar_max']]) 
colors = np.array(['#377eb8', '#ff7f00']) 
plt.scatter(x, y, alpha=0.4, c=colors[(y_pred + 1) // 2]) 
plt.xlabel('x1') 
plt.ylabel('x4')



pred=svm.predict(test[['txns', 'adr_per_tnx', 'dollar_mean', 'dollar_median', 'dollar_max']]) 
cnf_matrix = metrics.confusion_matrix(test[['exchange']], pred)
print(cnf_matrix)
print("Accuracy:",metrics.accuracy_score(test[['exchange']], pred))
print("Precision:",metrics.precision_score(test[['exchange']], pred))
print("Recall:",metrics.recall_score(test[['exchange']], pred))






'''Test'''
y_pred=clf.predict(X_test)
clf.score(X_test, y_test)



'''Confusion Matrix'''
from sklearn import metrics
cnf_matrix = metrics.confusion_matrix(y_test, y_pred)
print(cnf_matrix)
print("Accuracy:",metrics.accuracy_score(y_test, y_pred))
print("Precision:",metrics.precision_score(y_test, y_pred))
print("Recall:",metrics.recall_score(y_test, y_pred))


import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

class_names=[0,1] # name  of classes
fig, ax = plt.subplots()
tick_marks = np.arange(len(class_names))
plt.xticks(tick_marks, class_names)
plt.yticks(tick_marks, class_names)
# create heatmap
sns.heatmap(pd.DataFrame(cnf_matrix), annot=True, cmap="YlGnBu" ,fmt='g')
ax.xaxis.set_label_position("top")
plt.tight_layout()
plt.title('Confusion matrix', y=1.1)
plt.ylabel('Actual label')
plt.xlabel('Predicted label')


'''ROC Curve'''
y_pred_proba = clf.predict_proba(X_test)[::,1]
fpr, tpr, _ = metrics.roc_curve(y_test,  y_pred_proba)
auc = metrics.roc_auc_score(y_test, y_pred_proba)
plt.title('ROC Curve')
plt.plot(fpr,tpr,label="data 1, auc="+str(auc))
plt.legend(loc=4)
plt.show()


'''featrue importance'''
#https://stackoverflow.com/questions/44101458/random-forest-feature-importance-chart-using-python


'''Recall Precision Curce'''