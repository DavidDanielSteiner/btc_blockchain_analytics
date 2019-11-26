# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 17:58:31 2019

@author: nguye
"""

# -*- coding: utf-8 -*-
"""
Created on Sun Nov  3 11:36:01 2019

@author: nguye
"""
import dash
import dash_core_components as dcc
import dash_html_components as html
import numpy as np
import plotly.graph_objs as go
import datetime
from dash.dependencies import Output, Input
import pandas as pd
import plotly.express as px
import time

app = dash.Dash()

np.random.seed(50)
x_rand = np.random.randint(1,61,60)
y_rand = np.random.randint(1,61,60)

df = pd.read_csv("transactions_10MIO.csv")

"""weight = round(df.percent_marketcap,3)*1000
labels = []
for x in df.receiver_name:
    labels.append({'label':x,'value':x})
labels.append({'label':'all','value':'all'})
"""
################################### handling date times for sliders

daterange = pd.date_range(start=min([x for x in df['date']]),end=max([x for x in df['date']]))#,freq='Y')

def unixTimeMillis(dt):
    ''' Convert datetime to unix timestamp '''
    return int(time.mktime(dt.timetuple()))

def unixToDatetime(unix):
    ''' Convert unix timestamp to datetime. '''
    return pd.to_datetime(unix,unit='s')

def getMarks(start, end, Nth=100):
    ''' Returns the marks for labeling. 
        Every Nth value will be used.
    '''

    result = {}
    for i, date in enumerate(daterange):
        if(i%Nth == 1):
            # Append value to dict
            result[unixTimeMillis(date)] = str(date.strftime('%Y-%m-%d'))

    return result

#####################################

app.layout = html.Div([html.Div([html.H1("Wallet Explorer")], style={'textAlign': "center"}),
    html.Div([
        html.Div(
            dcc.Graph(
                id='scatter_chart',
                figure = {
                    # kann eigl weg
                    'data': go.Scatter(
                    ),
                    'layout' : go.Layout(
                        
                    )
                }
            )
        ),
        html.Label('date'),
        dcc.RangeSlider(
                id='slider_date',
                min = unixTimeMillis(daterange.min()),
                max = unixTimeMillis(daterange.max()),
                value = [unixTimeMillis(daterange.min()),
                         unixTimeMillis(daterange.max())],
                marks=getMarks(daterange.min(),
                            daterange.max()),
        ),
        
        
        
        
        
        
        

        html.Div(id='output-container-range-slider'),
    ],
        # not spanning the entire screen
    style={'width': '50%','display': 'inline-block'},
    )]
)

@app.callback(
    dash.dependencies.Output('scatter_chart', 'figure'),
    [dash.dependencies.Input('slider_date', 'value')],#,dash.dependencies.Input('slider_balance', 'value'),
    #dash.dependencies.Input('dropdown_type','value')],
    
)


def update_date(value1):#, value2,value_type):
    global df
    #df_filtered = df[df.receiver_name == value_type]
    #if value_type == 'all':
    #    df_filtered = df
    df_filtered = df #df_filtered[(pd.to_datetime(df_filtered.date) < unixToDatetime(value1[1])) & (pd.to_datetime(df_filtered.date) > unixToDatetime(value1[0]))]
    #df_filtered = df_filtered[(weight >= value2[0]) & (weight <= value2[1])]
    fig = go.Figure()
    fig.add_trace(
        go.Scattergl(
                 x = df_filtered.date,
                    y = df_filtered.dollar,
                    mode = 'markers',
                )
            )    

    
    """data = go.Scatter(
                    x = df_filtered.date,
                    y = df_filtered.dollar,
                    mode = 'markers',
                    ###
    )"""
    layout = go.Layout(
                        title = "Transactions",
                        xaxis = {'title' : 'Dollar'},
                        yaxis = {'title' : 'Date'},)
    
    #fig=go.Figure(data=data, layout=layout)
    return fig#print('From {} to {}'.format(unixToDatetime(value1[0]),unixToDatetime(value1[1])),value1)
        

if __name__ == "__main__":
    app.run_server(port=5000)
"""    
html.Br(),
        html.Label('market capitalization'),
        dcc.RangeSlider(
            id = 'slider_balance',
            min = min(weight),
            max = max(weight),
            value = [min(weight),max(weight)],
            step = 10,
            #marks = {i: i for i in range(max(df.weight))}
        ),
html.Br(),
html.Label('select owner type'),
dcc.Dropdown(
    id = 'dropdown_type',
    options = labels,
    value = 'all',
),


marker= {'size': [x for x in weight[(weight >= value2[0]) & (weight <= value2[1])]]}
"""


#print(len(df.receiver_name.unique()))