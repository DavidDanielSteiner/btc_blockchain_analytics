# -*- coding: utf-8 -*-
"""
Created on Sun Jan 19 14:31:38 2020

@author: David
"""


#https://towardsdatascience.com/autogluon-deep-learning-automl-5cdb4e2388ec


from autogluon import TabularPrediction as task
train_path = 'https://autogluon.s3.amazonaws.com/datasets/AdultIncomeBinaryClassification/train_data.csv'
train_data = task.Dataset(file_path=train_path)
predictor = task.fit(train_data, label='class', output_directory='ag-example-out/')




predictor = task.load('ag-example-out/')
test_path = 'https://autogluon.s3.amazonaws.com/datasets/AdultIncomeBinaryClassification/test_data.csv'
test_data = task.Dataset(file_path=test_path)
y_test = test_data['class']
test_data_nolabel = test_data.drop(labels=['class'],axis=1)
y_pred = predictor.predict(test_data_nolabel)
y_pred_proba = predictor.predict_proba(test_data_nolabel)
print(list(y_pred[:5]))
print(list(y_pred_proba[:5]))


leaderboard = predictor.leaderboard(test_data)