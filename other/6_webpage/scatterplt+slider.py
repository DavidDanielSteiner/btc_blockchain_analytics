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

df = pd.read_csv("testdata.csv")

################################### handling date times for sliders

daterange = pd.date_range(start=min([x for x in df['lastout']]),end=max([x for x in df['lastout']]))#,freq='D')

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
        html.Br(),
        html.Label('balance'),
        dcc.RangeSlider(
            id = 'slider_balance',
            min = min(df.weight),
            max = max(df.weight),
            value = [min(df.weight),max(df.weight)],
            step = 10,
            #marks = {i: i for i in range(max(df.weight))}
        ),
        html.Br(),
        html.Label('select owner type'),
        dcc.Dropdown(
            id = 'dropdown_type',
            options = [
                {'label' : 'Exchange', 'value' : 'exchange'},
                {'label' : 'Whale', 'value' : 'whale'},
                {'label' : 'Unknown', 'value' : 'unknown'},
                {'label' : 'All', 'value' : 'all'},
            ],
            value = 'exchange',
        ),
        html.Div(id='output-container-range-slider'),
    ],
        # not spanning the entire screen
    style={'width': '50%','display': 'inline-block'},
    )]
)

@app.callback(
    dash.dependencies.Output('scatter_chart', 'figure'),
    [dash.dependencies.Input('slider_date', 'value'),dash.dependencies.Input('slider_balance', 'value'),
    dash.dependencies.Input('dropdown_type','value')],
    
)

def update_date(value1, value2,value_type):
    global df
    df_filtered = df[df.category == value_type]
    if value_type == 'all':
        df_filtered = df
    df_filtered = df_filtered[(pd.to_datetime(df_filtered.lastout) < unixToDatetime(value1[1])) & (pd.to_datetime(df_filtered.lastout) > unixToDatetime(value1[0]))]
    df_filtered = df_filtered[(df_filtered.weight >= value2[0]) & (df_filtered.weight <= value2[1])]
    data = go.Scatter(
                    x = df_filtered.lastout,
                    y = df_filtered.firstin,
                    mode = 'markers',
                    marker= {'size': [x for x in df_filtered.weight[(df_filtered.weight >= value2[0]) & (df_filtered.weight <= value2[1])]]}
    )
    layout = go.Layout(
                        title = "Transactions",
                        xaxis = {'title' : 'First In'},
                        yaxis = {'title' : 'Last Out'},)
    fig=go.Figure(data=data, layout=layout)
    return fig#print('From {} to {}'.format(unixToDatetime(value1[0]),unixToDatetime(value1[1])),value1)
        

if __name__ == "__main__":
    app.run_server(port=4050)