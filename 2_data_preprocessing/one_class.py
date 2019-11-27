# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 21:00:26 2019

@author: David


https://www.kaggle.com/amarnayak/once-class-svm-to-detect-anomaly
http://rvlasveld.github.io/blog/2013/07/12/introduction-to-one-class-support-vector-machines/
https://towardsdatascience.com/outlier-detection-with-one-class-svms-5403a1a1878c
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager
from sklearn import svm

df = tmp3
df = df.reset_index(drop=True)


#df['adr_per_tnx'] = np.log(df['adr_per_tnx'])
#df['dollar_median'] = np.log(df['dollar_median'])

df['adr_per_tnx']=(df['adr_per_tnx']-df['adr_per_tnx'].mean())/df['adr_per_tnx'].std()
df['dollar_median']=(df['dollar_median']-df['dollar_median'].mean())/df['dollar_median'].std()
#log_data = np.log(df)


X_train = df[['adr_per_tnx', 'dollar_median']]
#df[df['category'] != 'Exchange']
#X_train = X_train[['adr_per_tnx', 'dollar_median']]
X_train = X_train.values

X_test = df[df['category'] != 'Exchange']
X_test = X_test[['adr_per_tnx', 'dollar_median']]
X_test = X_test.values

X_outliers = df[df['category'] == 'Exchange']
X_outliers = X_outliers[['adr_per_tnx', 'dollar_median']]
X_outliers = X_outliers.values



'''
xx, yy = np.meshgrid(np.linspace(-5, 5, 500), np.linspace(-5, 5, 500))
# Generate train data
X = 0.3 * np.random.randn(100, 2)
X_train = np.r_[X + 2, X - 2]
# Generate some regular novel observations
X = 0.3 * np.random.randn(20, 2)
X_test = np.r_[X + 2, X - 2]
# Generate some abnormal novel observations
X_outliers = np.random.uniform(low=-4, high=4, size=(20, 2))
'''



# fit the model
clf = svm.OneClassSVM(nu=0.1, kernel="rbf", gamma=0.1)
clf.fit(X_train)
y_pred_train = clf.predict(X_train)
y_pred_test = clf.predict(X_test)
y_pred_outliers = clf.predict(X_outliers)
n_error_train = y_pred_train[y_pred_train == -1].size
n_error_test = y_pred_test[y_pred_test == -1].size
n_error_outliers = y_pred_outliers[y_pred_outliers == 1].size

# plot the line, the points, and the nearest vectors to the plane
Z = clf.decision_function(np.c_[xx.ravel(), yy.ravel()])
Z = Z.reshape(xx.shape)

plt.figure(figsize=(20,20))
plt.title("exchange detection")
plt.contourf(xx, yy, Z, levels=np.linspace(Z.min(), 0, 7), cmap=plt.cm.PuBu)
a = plt.contour(xx, yy, Z, levels=[0], linewidths=2, colors='darkred')
plt.contourf(xx, yy, Z, levels=[0, Z.max()], colors='palevioletred')

s = 40
b1 = plt.scatter(X_train[:, 0], X_train[:, 1], c='white', s=s, edgecolors='k')
b2 = plt.scatter(X_test[:, 0], X_test[:, 1], c='blueviolet', s=s,
                 edgecolors='k')
c = plt.scatter(X_outliers[:, 0], X_outliers[:, 1], c='gold', s=s,
                edgecolors='k')
plt.axis('tight')
#plt.xlim((-1, 20))
#plt.ylim((-1, 20))
plt.legend([a.collections[0], b1, b2, c],
           ["learned frontier", "training observations",
            "new regular observations", "new abnormal observations"],
           loc="upper left",
           prop=matplotlib.font_manager.FontProperties(size=11))
plt.xlabel(
    "error train: %d ; errors novel regular: %d ; "
    "errors novel abnormal: %d"
    % (n_error_train, n_error_test, n_error_outliers))
plt.show()


