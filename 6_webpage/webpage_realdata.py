# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 20:06:38 2019

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

# embedding CCS file
external_stylesheets = ['assets/style.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

#app.css.append_css({
 #       'external_url': 'style.css',
#})
np.random.seed(50)
x_rand = np.random.randint(1,61,60)
y_rand = np.random.randint(1,61,60)

df = pd.read_csv("testdata2.csv")

# handling the point size
min_point_size = 5
step = (max(df.btc) - min(df.btc)) / 10 #step
add = round((df.btc - min(df.btc)) / step, 0)
df['point_size'] = min_point_size + add

# options in dropdown for owner
options = []
for owner in df.receiver_name2.unique():
    options.append({'label': owner, 'value': owner})
options.append({'label':'all','value':'all'})
    
########################################################################
# handling date times for sliders
########################################################################
daterange = pd.date_range(start=min(df.date),end=max(df.date))#,freq='Y')

def unixTimeMillis(dt):
    ''' Convert datetime to unix timestamp '''
    return int(time.mktime(dt.timetuple()))

def unixToDatetime(unix):
    ''' Convert unix timestamp to datetime. '''
    return pd.to_datetime(unix,unit='s')

def getMarks(start, end, Nth=300):
    ''' Returns the marks for labeling. 
        Every Nth value will be used.
    '''

    result = {}
    for i, date in enumerate(daterange):
        if(i%Nth == 1):
            # Append value to dict
            result[unixTimeMillis(date)] = str(date.strftime('%Y-%m-%d'))

    return result


############################################################
# Construction of Layout including plots
############################################################
    #put header oitside div
app.layout = html.Div([
        html.Header("Wallet Explorer"),
    #html.Div([
        html.Div([
            html.Br(),
            html.Br(),
            html.H2('Filters'),
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
            html.Label('market capitalization'),
            dcc.RangeSlider(
                id = 'slider_balance',
                min = min(df.point_size),
                max = max(df.point_size),
                value = [min(df.point_size),max(df.point_size)],
                step = 10,
                marks = {i: i for i in df.point_size}
            ),
            html.Br(),
            html.Label('select owner type'),
            dcc.Dropdown(
                id = 'dropdown_owner',
                options = options,
                value = 'all',
            ),html.Div(id= 'filters'),
        ],  #style = {'width': '100%','display': 'inline-block',}
        ),
        html.Div(
            dcc.Graph(
                id='scatter_chart',
            )
        ),
    #],
        # not spanning the entire screen
    #style = {'width': '100%','display': 'inline-block', 'font-family': 'Calibri',},
    
    #)
    ]
                
)
        
        
#################################################################
# Callbacks for Interaction
##################################################################

@app.callback(
    dash.dependencies.Output('scatter_chart', 'figure'),
    [dash.dependencies.Input('slider_date', 'value'),dash.dependencies.Input('slider_balance', 'value'),
    dash.dependencies.Input('dropdown_owner','value')],
    
)


def update_date(value1, value2, value3): 
    global df
    df_filtered = df
    df_filtered = df_filtered[(pd.to_datetime(df_filtered.date) < unixToDatetime(value1[1])) & (pd.to_datetime(df_filtered.date) > unixToDatetime(value1[0]))]
    df_filtered = df_filtered[(df_filtered.point_size >= value2[0]) & (df_filtered.point_size <= value2[1])]
    receiver_id, receivers = pd.factorize(df_filtered.receiver_name2)
    if value3 != 'all':
        df_filtered = df_filtered[df_filtered.receiver_name2 == value3] 
    fig = go.Figure(layout=go.Layout(
        title=go.layout.Title(text="Transactions"),
        xaxis = {'title' : 'timeline'},
        yaxis = {'title' : 'dollar'},)
    )
    fig.add_trace(
        go.Scattergl(
                    x = df_filtered.block_timestamp,
                    y = df_filtered.dollar,
                    text = df_filtered.receiver,
                    mode = 'markers',
                    #alternative: use sizescr, sizemode, sizeref
                    marker_color = receiver_id,
                    marker=  {'size': [x for x in df_filtered.point_size[(df_filtered.point_size >= value2[0]) & (df_filtered.point_size <= value2[1])]],},
        ),
    )    
    return fig
        


if __name__ == "__main__":
    app.run_server(port=8000)


