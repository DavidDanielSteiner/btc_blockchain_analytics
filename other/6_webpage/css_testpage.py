# -*- coding: utf-8 -*-
"""
Created on Sat Dec  7 15:45:54 2019

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


external_stylesheets = ['assets/style.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
        html.Div([html.Header("1")]),
        html.Div([html.Header("2")]),
        html.Div([html.Header("3")]),
        html.Div([html.Header("4")]),
        html.Div([html.Header("5")]),
        html.Div([html.Header("6")]),
        html.Div([html.Header("7")]),
        html.Div([html.Header("8")]),
        html.Div([html.Header("9")]),
        html.Div([html.Header("7")]),
        html.Div([html.Header("8")]),
        html.Div([html.Header("9")]),
        html.Div([html.Header("Wallet Explorer")]),
        html.Div(),
        html.Div([html.Header("Filters")]),
        html.Div([]),
        html.Div([html.Header("Wallet Information")]),
                            
 
        
], className="container",             
)
        

if __name__ == "__main__":
    app.run_server(port=8000)
